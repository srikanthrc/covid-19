from datetime import datetime
import pandas as pd
import getpass


base_url = 'https://raw.githubusercontent.com/srikanthrc/covid-19/master/'
base_url = '' if (getpass.getuser() == 'Pratap Vardhan') else base_url
paths = {
    'mapping': base_url + 'mapping_countries.csv',
	'states': base_url + 'mapping_states.csv',
    'overview': base_url + 'overview.tpl'
}


def get_mappings(url):
    df = pd.read_csv(url, encoding='utf-8')
    return {
        'df': df,
        'replace.country': dict(df.dropna(subset=['Name']).set_index('Country')['Name']),
        'map.continent': dict(df.set_index('Name')['Continent'])
    }

def get_states(url):
    df = pd.read_csv(url, encoding='utf-8')
    return {
        'df': df,
		'replace.state': dict(df.dropna(subset=["Name"]).set_index('State')['Name'])
    }

mapping = get_mappings(paths['mapping'])
states = get_states(paths['states'])


def get_template(path):
    from urllib.parse import urlparse
    if bool(urlparse(path).netloc):
        from urllib.request import urlopen
        return urlopen(path).read().decode('utf8')
    return open(path, encoding='utf8').read()


def get_frame(name):
    url = (
        'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/'
        f'csse_covid_19_time_series/time_series_19-covid-{name}.csv')
    df = pd.read_csv(url, encoding='utf-8')
    # rename countries
    df['Country/Region'] = df['Country/Region'].replace(mapping['replace.country'])
    return df


def get_covidtracking_data(name):
	url = (
	    'https://covidtracking.com/api/states/'
		f'daily.csv')
	data = pd.read_csv(url, encoding='utf-8')
	# convert to JHU format
	# data['date'] = pd.to_datetime(data['date'], format="%Y%m%d")
	# data = data.rename(columns={"state": "Province/State"})
	data['Date'] = data['date'].apply(lambda x: datetime.strptime(str(x),'%Y%m%d').strftime("%m/%d/%y"))
	data['Province/State'] = data['state'].replace(states['replace.state'])
	df = data.pivot(index="Province/State", columns="Date", values=name)
	df['Country/Region'] = "US"
	df['Lat'] = "NaN"
	df['Long'] = "NaN"
	return df


def get_dates(df):
    dt_cols = df.columns[~df.columns.isin(['Province/State', 'Country/Region', 'Lat', 'Long'])]
    latest_date_idx = -1
    # sometimes last column may be empty, then go backwards
    for i in range(-1, -len(dt_cols), -1):
        if not df[dt_cols[i]].fillna(0).eq(0).all():
            latest_date_idx = i
            break
    return latest_date_idx, dt_cols


def gen_data(region='Country/Region', filter_frame=lambda x: x, add_table=[], kpis_info=[]):
    col_region = region
    df = get_frame('Confirmed')

    latest_date_idx, dt_cols = get_dates(df)
    dt_today = dt_cols[latest_date_idx]
    dt_ago = dt_cols[latest_date_idx - 1]

    dft_cases = df.pipe(filter_frame)
    dfc_cases = dft_cases.groupby(col_region)[dt_today].sum()
    dfp_cases = dft_cases.groupby(col_region)[dt_ago].sum()

    dft_deaths = get_frame('Deaths').pipe(filter_frame)
    dfc_deaths = dft_deaths.groupby(col_region)[dt_today].sum()
    dfp_deaths = dft_deaths.groupby(col_region)[dt_ago].sum()

    dft_recovered = get_frame('Recovered').pipe(filter_frame)
    dfc_recovered = dft_recovered.groupby(col_region)[dt_today].sum()
    dfp_recovered = dft_recovered.groupby(col_region)[dt_ago].sum()

    df_table = (pd.DataFrame(dict(
        Cases=dfc_cases, Deaths=dfc_deaths, Recovered=dfc_recovered,
        PCases=dfp_cases, PDeaths=dfp_deaths, PRecovered=dfp_recovered))
        .sort_values(by=['Cases', 'Deaths'], ascending=[False, False])
        .reset_index())
    for c in 'Cases, Deaths, Recovered'.split(', '):
        df_table[f'{c} (+)'] = (df_table[c] - df_table[f'P{c}']).clip(0)  # DATABUG
    df_table['Fatality Rate'] = (100 * df_table['Deaths'] / df_table['Cases']).round(1)

    for rule in add_table:
        df_table[rule['name']] = df_table.pipe(rule['apply'])

    def kpi_of(name, prefix, pipe):
        df_f = df_table.pipe(pipe or (lambda x: x[x[col_region].eq(name)]))
        return df_f[metrics].sum().add_prefix(prefix)

    metrics = ['Cases', 'Deaths', 'Recovered', 'Cases (+)', 'Deaths (+)', 'Recovered (+)']
    s_kpis = pd.concat([
        kpi_of(x['title'], f'{x["prefix"]} ', x.get('pipe'))
        for x in kpis_info])
    summary = {'updated': pd.to_datetime(dt_today), 'since': pd.to_datetime(dt_ago)}
    summary = {**summary, **df_table[metrics].sum(), **s_kpis}
    dft_ct_cases = dft_cases.groupby(col_region)[dt_cols].sum()
    dft_ct_new_cases = dft_ct_cases.diff(axis=1).fillna(0).astype(int)
    return {
        'summary': summary, 'table': df_table, 'newcases': dft_ct_new_cases,
        'dt_last': latest_date_idx, 'dt_cols': dt_cols}



def states_data(region='Province/State', filter_frame=lambda x: x, add_table=[], kpis_info=[]):
	col_region = region
	df = get_covidtracking_data('positive')

	latest_date_idx, dt_cols = get_dates(df)
	dt_today = dt_cols[latest_date_idx]
	dt_ago = dt_cols[latest_date_idx - 1]

	dft_cases = df.pipe(filter_frame)
	dfc_cases = dft_cases.groupby(col_region)[dt_today].sum()
	dfp_cases = dft_cases.groupby(col_region)[dt_ago].sum()

	dft_deaths = get_covidtracking_data('death').pipe(filter_frame)
	dfc_deaths = dft_deaths.groupby(col_region)[dt_today].sum()
	dfp_deaths = dft_deaths.groupby(col_region)[dt_ago].sum()

	df_table = (pd.DataFrame(dict(
		Cases=dfc_cases, Deaths=dfc_deaths,
		PCases=dfp_cases, PDeaths=dfp_deaths))
		.sort_values(by=['Cases', 'Deaths'], ascending=[False, False])
		.reset_index())

	for c in 'Cases, Deaths'.split(', '):
		df_table[f'{c} (+)'] = (df_table[c] - df_table[f'P{c}']).clip(0)  # DATABUG
	df_table['Fatality Rate'] = (100 * df_table['Deaths'] / df_table['Cases']).round(1)

	for rule in add_table:
		df_table[rule['name']] = df_table.pipe(rule['apply'])

	def kpi_of(name, prefix, pipe):
		df_f = df_table.pipe(pipe or (lambda x: x[x[col_region].eq(name)]))
		return df_f[metrics].sum().add_prefix(prefix)

	metrics = ['Cases', 'Deaths', 'Cases (+)', 'Deaths (+)']
	s_kpis = pd.concat([
		kpi_of(x['title'], f'{x["prefix"]} ', x.get('pipe'))
		for x in kpis_info])

	summary = {'updated': pd.to_datetime(dt_today), 'since': pd.to_datetime(dt_ago)}
	summary = {**summary, **df_table[metrics].sum(), **s_kpis}
	dft_ct_cases = dft_cases.groupby(col_region)[dt_cols].sum()
	dft_ct_new_cases = dft_ct_cases.diff(axis=1).fillna(0).astype(int)
	return {
		'summary': summary, 'table': df_table, 'newcases': dft_ct_new_cases,
		'dt_last': latest_date_idx, 'dt_cols': dt_cols}


if __name__ == "__main__":
	kpis_info = [
		{'title': 'New York', 'prefix': 'NY'},
		{'title': 'Washington', 'prefix': 'WA'},
		{'title': 'California', 'prefix': 'CA'}]

	data = states_data(kpis_info=kpis_info)
	print(data['summary'])