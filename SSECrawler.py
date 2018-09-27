import time
import requests
import json
from random import random
import os.path
from datetime import datetime
def _download_pdf_by_url(url, filename='./temp.pdf'):
    response = requests.get(url)        
    if not os.path.isfile(filename):
        with open(filename, 'wb') as f:
            f.write(response.content)

def crawl_sse_inquiry_letters(start_page=1, end_page=1, is_download_letters=False, download_directory='./', print_log=False, file_name='demo.csv'):
    '''
        is_down_load_letters: choose if download all the inquiry letters pdf files
    '''
    headers_inquiry_letters = {'Host':"query.sse.com.cn",
                               'Referer':"http://www.sse.com.cn/disclosure/credibility/supervision/inquiries/",
                               'Accept':'*/*', 
                               'UserAgent':'"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36"'}
    url_inquiry_letters = "http://query.sse.com.cn/commonSoaQuery.do?jsonCallBack={2}&siteId=28&sqlId=BS_GGLL&extGGLX=&stockcode=&channelId=10743%2C10744%2C10012&extGGDL=&order=createTime%7Cdesc%2Cstockcode%7Casc&isPagination=true&pageHelp.pageSize=15&pageHelp.pageNo={0}&pageHelp.beginPage={0}&pageHelp.cacheSize=1&pageHelp.endPage=21&_={1}"

    columns_match_dict={'Ticker':'stockcode',
                        'Company_name':'extGSJC',
                        'Date':'cmsOpDate',
                        'Inquiry_type':'extWTFL',
                        'Title':'docTitle',
                        'Doc_url':'docURL'}

    epoch_time=int(time.time())
    jscallback_patern=str(int(random()*10000))

    lines=[]
    line=''
    for k in columns_match_dict.keys():
        line+=k+','
    lines.append(line.rstrip(','))
    for page_num in range(start_page,end_page+1):
        try:
            if print_log:
                print('Start crawling page {}'.format(page_num))
            url=url_inquiry_letters.format(page_num,epoch_time,jscallback_patern)
            response = requests.get(url=url,headers=headers_inquiry_letters)
            json_object=json.loads(response.text[len(jscallback_patern)+1:len(response.text)-1])
            row_count=len(json_object['pageHelp']['data'])
            for i in range(row_count):
                line=''
                for k in columns_match_dict.keys():
                    line+=json_object['pageHelp']['data'][i][columns_match_dict[k]]+','
                lines.append(line.rstrip(','))
        except Exception as e:
            print(e)

    try:
        with open(file_name,'w') as file:
            for line in lines:
                file.write(line)
                file.write('\n')
    except Exception as e:
        print(e)

    if len(lines)>1:
        if is_download_letters:
            for line in lines[1:]:
                row=line.split(',')
                try:
                    url='http://'+row[5]                   
                    filename='{}_{}_{}_{}.pdf'.format(datetime.strptime(row[2], '%Y-%m-%d %H:%M:%S').strftime("%Y%m%d"),
                                                      row[0], 
                                                      row[1].replace('*',''),
                                                      row[4])
                    if print_log:
                        print('Start downloading PDF file: {}'.format(filename))
                    _download_pdf_by_url(url,download_directory+filename)
                except Exception as e:
                    print(e)

    return lines

if __name__ == '__main__':
    t_0=time.time()
    df=crawl_sse_inquiry_letters(start_page=1,end_page=1,is_download_letters=False, print_log=True)
    t_1=time.time()
    print('finish, cost {:.3f} seconds'.format(t_1-t_0))
    # Start crawling page 1
    # finish, cost 1.160 seconds
    #     Ticker	Company_name	Date	Inquiry_type	Title	Doc_url
    # 0	600678	四川金顶	2018-08-14 18:40:01	重大资产重组预案审核意见函	关于四川金顶(集团)股份有限公司的重大资产重组购买报告书(草案）信息披露的二次问询函	www.sse.com.cn/disclosure/credibility/supervis...
    # 1	600478	科力远	2018-08-14 17:40:01	问询函	关于湖南科力远新能源股份有限公司的问询函	www.sse.com.cn/disclosure/credibility/supervis...
    # 2	600022	山东钢铁	2018-08-13 20:10:01	问询函	关于对山东钢铁股份有限公司终止重大资产重组事项的问询函	www.sse.com.cn/disclosure/credibility/supervis...
    # 3	601390	中国中铁	2018-08-13 18:20:01	重大资产重组预案审核意见函	关于中国中铁股份有限公司的重大资产重组预案信息披露的问询函	www.sse.com.cn/disclosure/credibility/supervis...
    # 4	600136	当代明诚	2018-08-10 17:10:01	问询函	关于武汉当代明诚文化股份有限公司重大资产购买进展事项的问询函	www.sse.com.cn/disclosure/credibility/supervis...