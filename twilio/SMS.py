import smtplib
from configparser import ConfigParser

carriers = {
    'att': '@txt.att.net',
    'tmobile': '@tmomail.net',
    'verizon': '@vtext.com',
    'sprint': '@messaging.sprintpcs.com',
    'boost-mobile': '@smsmyboostmobile.com',
    'cricket': '@sms.cricketwireless.net',
    'us-cellular': '@email.uscc.net'
}


def config(filename='/home/lermane/Documents/ulta_secret_sales/twilio/SMS.ini', section='gmail'):
    """ this function parses the twilio.ini file and returns it in dictionary form """
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section, default to postgresql
    gm = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            gm[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return gm

def send(name, message):
        # Replace the number with your own, or consider using an argument\dict for multiple people.
    if name == 'michael':
        to_number = '7347888832@tmomail.net'
    else:
        to_number = '7343634534@tmomail.net'
    auth = config()

    # Establish a secure session with gmail's outgoing SMTP server using your gmail account
    server = smtplib.SMTP("smtp.gmail.com", 587)
    server.starttls()
    server.login(auth['email'], auth['password'])

    # Send text message through SMS gateway of destination number
    server.sendmail(auth['email'], to_number, message)