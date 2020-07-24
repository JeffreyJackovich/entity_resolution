from pandas_profiling import ProfileReport
from entity_resolution import config

def get_profile_report(df):
    profile = df.profile_report(title='Pandas Profiling Report',
                            minimal=True)
    # json_data = profile.to_json() # As a string
    # profile.to_file("your_report.json")
    profile.to_file(config.ABS_PATH + config.REPORT_DIR + "pandas_profiling_report.html")
    return profile

