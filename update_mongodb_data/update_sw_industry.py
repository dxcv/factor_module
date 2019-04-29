from download_joinquant_data_to_mongodb.get_sw_industry_from_joinquant import *


if __name__=='__main__':
    m=MongoDB_io()
    m.remove_all_documents_from_mongodb()
    get_sw_industry()
    pass


