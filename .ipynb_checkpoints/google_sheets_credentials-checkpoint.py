def get_credentials_file(t):
    if t == 'main_mod':
        return(r'credentials/mod_credentials.json')
    if t == 'main_local':
        return(r'credentials/unmindful_credentials.json')
    if t == 'test':
        return(r'credentials/unmindful_credentials.json')
    
def get_token_write_file(t):
    if t == 'main_mod':
        return(r'credentials/mod_token_write.pickle')
    if t == 'main_local':
        return(r'credentials/unmindful_token_write.pickle')
    if t == 'test':
        return(r'credentials/test_token_write.pickle')

def get_sheet_id(t):
    if t == 'main_mod':
        return('16-aJIGT4NZAxVfcix5whLo95WuChzfO9itpuS75rlB4')
    if t == 'main_local':
        return('1UOg1KoDT72rGWOS4P8j7tL1xHaWt0hJtYe23OBIyhbw')
    if t == 'test':
        return('1xjdPm0k3qvNR5LgbdxUmMqnSkGT5DYnGxNlZc6swPOU')
    
def get_filter_id(t):
    if t == 'main_mod':
        return('2092242562')
    if t == 'main_local':
        return('511550738')
    if t == 'test':
        return('1349307930')
    
def get_sheet_name(t):
    if t == 'main_mod':
        return('Sheet1')
    if t == 'main_local':
        return('secret_sales')
    if t == 'test':
        return('Sheet1')