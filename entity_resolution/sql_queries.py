
drop_raw_table = ('''
    DROP TABLE IF EXISTS raw_table
''')

drop_donors = ('''
    DROP TABLE IF EXISTS donors
''')

drop_recipients = ('''
    DROP TABLE IF EXISTS recipients
''')

drop_contributions = ('''
    DROP TABLE IF EXISTS contributions
''')

drop_processed_donors = ('''
    DROP TABLE IF EXISTS processed_donors
''')

create_raw_table = ('''
        CREATE TABLE raw_table
        (reciept_id INT, last_name VARCHAR(70), first_name VARCHAR(35),
        address_1 VARCHAR(35), address_2 VARCHAR(36), city VARCHAR(20),
        state VARCHAR(15), zip VARCHAR(11), report_type VARCHAR(24),
        date_recieved VARCHAR(10), loan_amount VARCHAR(12),
        amount VARCHAR(23), receipt_type VARCHAR(23),
        employer VARCHAR(70), occupation VARCHAR(40),
        vendor_last_name VARCHAR(70), vendor_first_name VARCHAR(20),
        vendor_address_1 VARCHAR(35), vendor_address_2 VARCHAR(31),
        vendor_city VARCHAR(20), vendor_state VARCHAR(10),
        vendor_zip VARCHAR(10), description VARCHAR(90),
        election_type VARCHAR(10), election_year VARCHAR(10),
        report_period_begin VARCHAR(10), report_period_end VARCHAR(33),
        committee_name VARCHAR(70), committee_id VARCHAR(37))
''')

'''SELECT name, COUNT(*)
    FROM processed_donors 
   'GROUP BY name '
   'HAVING COUNT(*) > 1'
   'ORDER BY count '
   'DESC'''

# class SqlQuery:
#     """ Exporting queries"""
#     def __init__(self, drop_query):
#         self.drop_query = drop_query
#
# # creates instances for SqlQuery class
# drop_query = SqlQuery()

# store as a list for iteration
drop_queries = [drop_raw_table, drop_donors, drop_recipients, drop_contributions, drop_processed_donors]
