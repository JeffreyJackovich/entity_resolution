from dataset import Dataset
from os import path
from postgres import Postgres
from psycopg2 import DatabaseError
import dj_database_url

ABS_PATH = path.abspath('..')

EXTERNAL_DATA_DIR = '/data/external/'
CONTRIBUTIONS_PATH = ABS_PATH + EXTERNAL_DATA_DIR

_file = 'Illinois-campaign-contributions'
contributions_csv_file = CONTRIBUTIONS_PATH + _file + '.csv'


def main():
    print('Starting..')
    print('Downloading the dataset..')
    source_dataset = Dataset(CONTRIBUTIONS_PATH)
    source_dataset.download_file()
    source_dataset.extract_file()
    source_dataset.convert_file()

    # TODO: configure check to validate db env variables are configured
    # db_conf = dj_database_url.config()
    #
    # if not db_conf:
    #     raise Exception(
    #         'set DATABASE_URL environment variable with your connection, e.g. '
    #         'export DATABASE_URL=postgres://user:password@host/mydatabase'
    #     )
    #
    # print(db_conf)
    # db = Postgres()


    # TODO: verify schema name
    # schema_name = 'il_campaign_contribution'
    #
    # print('Creating the Schema..')
    # create_db_query = "CREATE SCHEMA IF NOT EXISTS entity_resolution;"
    # db.create_database(create_db_query)


    # drop_raw_table = ('''
    #     DROP TABLE IF EXISTS raw_table
    # ''')
    #
    # drop_donors = ('''
    #     DROP TABLE IF EXISTS donors
    # ''')
    #
    # drop_recipients = ('''
    #     DROP TABLE IF EXISTS recipients
    # ''')
    #
    # drop_contributions = ('''
    #     DROP TABLE IF EXISTS contributions
    # ''')
    #
    # drop_processed_donors = ('''
    #     DROP TABLE IF EXISTS processed_donors
    # ''')
    #
    # print('Dropping tables if exist..')
    # drop_queries = [drop_raw_table, drop_donors, drop_recipients, drop_contributions, drop_processed_donors]
    # for drop_query in drop_queries:
    #     db.execute_query(drop_query)
    #
    # print('Creating the tables..')
    #
    # try:
    #     db.execute_query('''CREATE TABLE raw_table
    #               (reciept_id INT, last_name VARCHAR(70), first_name VARCHAR(35),
    #                address_1 VARCHAR(35), address_2 VARCHAR(36), city VARCHAR(20),
    #                state VARCHAR(15), zip VARCHAR(11), report_type VARCHAR(24),
    #                date_recieved VARCHAR(10), loan_amount VARCHAR(12),
    #                amount VARCHAR(23), receipt_type VARCHAR(23),
    #                employer VARCHAR(70), occupation VARCHAR(40),
    #                vendor_last_name VARCHAR(70), vendor_first_name VARCHAR(20),
    #                vendor_address_1 VARCHAR(35), vendor_address_2 VARCHAR(31),
    #                vendor_city VARCHAR(20), vendor_state VARCHAR(10),
    #                vendor_zip VARCHAR(10), description VARCHAR(90),
    #                election_type VARCHAR(10), election_year VARCHAR(10),
    #                report_period_begin VARCHAR(10), report_period_end VARCHAR(33),
    #                committee_name VARCHAR(70), committee_id VARCHAR(37))''')
    #
    #     print("Table created successfully in PostgreSQL ")
    # except (Exception, DatabaseError) as error:
    #     print("Error while creating PostgreSQL table", error)
    # finally:
    #     # TODO: add close db
    #     print("PostgreSQL connection is closed")
    #
    # print('importing raw data from csv...')
    #
    # with open(contributions_csv_file, 'r') as csv_file:
    #     db.copy_csv_query("COPY raw_table "
    #                        "(reciept_id, last_name, first_name, "
    #                        " address_1, address_2, city, state, "
    #                        " zip, report_type, date_recieved, "
    #                        " loan_amount, amount, receipt_type, "
    #                        " employer, occupation, vendor_last_name, "
    #                        " vendor_first_name, vendor_address_1, "
    #                        " vendor_address_2, vendor_city, vendor_state, "
    #                        " vendor_zip, description, election_type, "
    #                        " election_year, "
    #                        " report_period_begin, report_period_end, "
    #                        " committee_name, committee_id) "
    #                        "FROM STDIN CSV HEADER", csv_file)
    #
    #
    #
    # print('creating donors table...')
    # db.execute_query("CREATE TABLE donors "
    #          "(donor_id SERIAL PRIMARY KEY, "
    #          " last_name VARCHAR(70), first_name VARCHAR(35), "
    #          " address_1 VARCHAR(35), address_2 VARCHAR(36), "
    #          " city VARCHAR(20), state VARCHAR(15), "
    #          " zip VARCHAR(11), employer VARCHAR(70), "
    #          " occupation VARCHAR(40))")
    #
    # db.update_rows("INSERT INTO donors "
    #          "(first_name, last_name, address_1, "
    #          " address_2, city, state, zip, employer, occupation) "
    #          "SELECT DISTINCT "
    #          "LOWER(TRIM(first_name)), LOWER(TRIM(last_name)), "
    #          "LOWER(TRIM(address_1)), LOWER(TRIM(address_2)), "
    #          "LOWER(TRIM(city)), LOWER(TRIM(state)), LOWER(TRIM(zip)), "
    #          "LOWER(TRIM(employer)), LOWER(TRIM(occupation)) "
    #          "FROM raw_table")
    #
    #
    # print('creating indexes on donors table...')
    # db.execute_query("CREATE INDEX donors_donor_info ON donors "
    #          "(last_name, first_name, address_1, address_2, city, "
    #          " state, zip)")
    #
    # print('creating recipients table...')
    # db.execute_query("CREATE TABLE recipients "
    #          "(recipient_id SERIAL PRIMARY KEY, name VARCHAR(70))")
    #
    # db.update_rows("INSERT INTO recipients "
    #          "SELECT DISTINCT CAST(committee_id AS INTEGER), "
    #          "committee_name FROM raw_table")
    #
    #
    # print('creating contributions table...')
    # db.execute_query("CREATE TABLE contributions "
    #          "(contribution_id INT, donor_id INT, recipient_id INT, "
    #          " report_type VARCHAR(24), date_recieved DATE, "
    #          " loan_amount VARCHAR(12), amount VARCHAR(23), "
    #          " receipt_type VARCHAR(23), "
    #          " vendor_last_name VARCHAR(70), "
    #          " vendor_first_name VARCHAR(20), "
    #          " vendor_address_1 VARCHAR(35), vendor_address_2 VARCHAR(31), "
    #          " vendor_city VARCHAR(20), vendor_state VARCHAR(10), "
    #          " vendor_zip VARCHAR(10), description VARCHAR(90), "
    #          " election_type VARCHAR(10), election_year VARCHAR(10), "
    #          " report_period_begin DATE, report_period_end DATE)")
    #
    # db.update_rows("INSERT INTO contributions "
    #          "SELECT reciept_id, donors.donor_id, CAST(committee_id AS INTEGER), "
    #          " report_type, TO_DATE(TRIM(date_recieved), 'MM/DD/YYYY'), "
    #          " loan_amount, amount, "
    #          " receipt_type, vendor_last_name , "
    #          " vendor_first_name, vendor_address_1,"
    #          " vendor_address_2, "
    #          " vendor_city, vendor_state, vendor_zip,"
    #          " description, "
    #          " election_type, election_year, "
    #          " TO_DATE(TRIM(report_period_begin), 'MM/DD/YYYY'), "
    #          " TO_DATE(TRIM(report_period_end), 'MM/DD/YYYY') "
    #          "FROM raw_table JOIN donors ON "
    #          "donors.first_name = LOWER(TRIM(raw_table.first_name)) AND "
    #          "donors.last_name = LOWER(TRIM(raw_table.last_name)) AND "
    #          "donors.address_1 = LOWER(TRIM(raw_table.address_1)) AND "
    #          "donors.address_2 = LOWER(TRIM(raw_table.address_2)) AND "
    #          "donors.city = LOWER(TRIM(raw_table.city)) AND "
    #          "donors.state = LOWER(TRIM(raw_table.state)) AND "
    #          "donors.employer = LOWER(TRIM(raw_table.employer)) AND "
    #          "donors.occupation = LOWER(TRIM(raw_table.occupation)) AND "
    #          "donors.zip = LOWER(TRIM(raw_table.zip))")
    #
    # print('creating indexes on contributions...')
    # db.execute_query("ALTER TABLE contributions ADD PRIMARY KEY(contribution_id)")
    # db.execute_query("CREATE INDEX donor_idx ON contributions (donor_id)")
    # db.execute_query("CREATE INDEX recipient_idx ON contributions (recipient_id)")
    #
    #
    # print('nullifying empty strings in donors...')
    # db.update_rows(
    #     "UPDATE donors "
    #     "SET "
    #     "first_name = CASE first_name WHEN '' THEN NULL ELSE first_name END, "
    #     "last_name = CASE last_name WHEN '' THEN NULL ELSE last_name END, "
    #     "address_1 = CASE address_1 WHEN '' THEN NULL ELSE address_1 END, "
    #     "address_2 = CASE address_2 WHEN '' THEN NULL ELSE address_2 END, "
    #     "city = CASE city WHEN '' THEN NULL ELSE city END, "
    #     "state = CASE state WHEN '' THEN NULL ELSE state END, "
    #     "employer = CASE employer WHEN '' THEN NULL ELSE employer END, "
    #     "occupation = CASE occupation WHEN '' THEN NULL ELSE occupation END, "
    #     "zip = CASE zip WHEN '' THEN NULL ELSE zip END"
    # )
    #
    #
    #
    # print('creating processed_donors...')
    # db.execute_query("CREATE TABLE processed_donors AS "
    #          "(SELECT donor_id, "
    #          " LOWER(city) AS city, "
    #          " CASE WHEN (first_name IS NULL AND last_name IS NULL) "
    #          "      THEN NULL "
    #          "      ELSE LOWER(CONCAT_WS(' ', first_name, last_name)) "
    #          " END AS name, "
    #          " LOWER(zip) AS zip, "
    #          " LOWER(state) AS state, "
    #          " CASE WHEN (address_1 IS NULL AND address_2 IS NULL) "
    #          "      THEN NULL "
    #          "      ELSE LOWER(CONCAT_WS(' ', address_1, address_2)) "
    #          " END AS address, "
    #          " LOWER(occupation) AS occupation, "
    #          " LOWER(employer) AS employer, "
    #          " CAST((first_name IS NULL) AS INTEGER) AS person "
    #          " FROM donors)")
    #
    # db.execute_query("CREATE INDEX processed_donor_idx ON processed_donors (donor_id)")
    #
    # # TODO: close db connection
    # db.con.close()
    #
    # print('done')









    # df = pd.read_sql_query('SELECT * '
    #                        'FROM processed_donors ', con=con_obj)

    # df = pd.read_sql_query('SELECT COUNT(name) '
    #                        'FROM processed_donors ',con=con_obj) # 706,030
    #

    # df = pd.read_sql_query('SELECT COUNT(DISTINCT name) '
    #                        'FROM processed_donors ',con=con_obj) # 432,201

    #
    # start_time = time.time()
    # df = pd.read_sql_query(', con=con_obj)
    # print("--- %s seconds ---" % (time.time() - start_time))

if __name__ == "__main__":
    main()