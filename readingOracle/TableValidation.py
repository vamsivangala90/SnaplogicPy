import cx_Oracle


def connect_to_oracle():
    # Specify the connection details
    host = 'oratestdb2.cwztruwzzvnq.us-east-1.rds.amazonaws.com'
    port = '1521'
    service_name = 'TESTDB'

    # Create a DSN using cx_Oracle.makedsn
    dsn = cx_Oracle.makedsn(host, port, service_name)

    # Now you can use the DSN to establish a connection to the Oracle database
    # Replace 'username' and 'password' with your actual Oracle database credentials
    connection = cx_Oracle.connect('PRASANNA$#_', '1nt3grat10n', dsn)

    # Remember to close the connection when done
    connection.close()


# Call the function to test it
connect_to_oracle()
