import requests
import polars
import time


# resp=requests.get(url)
# for i in range(1,2734):
    
    

def get_result_megasena(contest_number):
    url='https://servicebus2.caixa.gov.br/portaldeloterias/api/megasena/'
    resp=requests.get(url+str(contest_number))
    if resp.status_code==200:
        print('status code')
        print(resp.status_code)
        resp=resp.json()
        return resp
    return None

data=[]
for i in range(1,2734):
    print(i)
    result=get_result_megasena(i)
    while result is None:
        time.sleep(1)
        result=get_result_megasena(i)

    data.append(result)
        

pdf=polars.from_dicts(data)
pdf.write_parquet('dados_mega_sena.parquet',compression='uncompressed')

print(pdf.head(2))


# print(get_result_megasena(2733))
