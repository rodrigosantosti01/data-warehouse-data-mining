import csv
import tempfile
from locale import normalize
import pandas as pd
import mysql.connector as mysql
import numpy as np
import tempfile
import unidecode


def get_connect():
    db = mysql.connect(
        host="localhost",
        user="root",
        passwd="root",
        database="dwh"
    )
    return db


def extract_lancamento(name_file):
    db = get_connect()
    splited= name_file.split("/")
    splited = splited[2].split("_")
    data = (splited[0],)
    try:
        cursor = db.cursor()
        lancamento_query = "insert into lancamento (id,data_lancamento) values (null,%s)"
        cursor.execute(lancamento_query,data)
        db.commit()
    except Exception as e:
        print("erro",e)
        pass

def extract_instituicao(name_file):
    db = get_connect()
    data = pd.read_csv(name_file, encoding='utf8', sep=";")
    data = data[["CODIGO ORGAO SUPERIOR", "NOME ORGAO SUPERIOR", "CODIGO ORGAO SUBORDINADO",
                 "NOME ORGAO SUBORDINADO", "CODIGO UNIDADE ORCAMENTARIA", "NOME UNIDADE ORCAMENTARIA"]]

    data = data.drop_duplicates(subset=["CODIGO ORGAO SUPERIOR","CODIGO ORGAO SUBORDINADO","CODIGO UNIDADE ORCAMENTARIA"])
    contador = 0
    for x in data.values:
        tuplas=tuple(x) 
        try:
            cursor = db.cursor()
            query = "insert into instituicao(id, id_orgao, nome_orgao, id_orgao_subordinado, nome_orgao_subordinado, id_unidade_gestora, nome_unidade_gestora) values (null,%s,%s,%s,%s,%s,%s); "
            cursor.execute(query, tuplas)
            db.commit()
            contador = contador + 1
        except Exception as e:
            print("erro",e)
            # print("tuplas",tuplas)
            pass
    
    print(contador, "instituição inserted")


def extract_natureza_despesa(name_file,d_o):
    db = get_connect()

    data = pd.read_csv(name_file, encoding='utf8', sep=";")
    if d_o == "d":
        
        data = data[["CODIGO GRUPO DE DESPESA", "NOME GRUPO DE DESPESA", "CODIGO ELEMENTO DE DESPESA", "NOME ELEMENTO DE DESPESA",
                    "CODIGO MODALIDADE DA DESPESA", "MODALIDADE DA DESPESA"]]

        data = data.drop_duplicates(subset=['CODIGO GRUPO DE DESPESA', 'CODIGO ELEMENTO DE DESPESA','CODIGO MODALIDADE DA DESPESA'])
    else:
        data = data[["CODIGO GRUPO DE DESPESA", "NOME GRUPO DE DESPESA", "CODIGO ELEMENTO DE DESPESA", "NOME ELEMENTO DE DESPESA",
                    "CODIGO CATEGORIA ECONOMICA","NOME CATEGORIA ECONOMICA"]]
        data = data.drop_duplicates(subset=['CODIGO GRUPO DE DESPESA', 'CODIGO ELEMENTO DE DESPESA','CODIGO CATEGORIA ECONOMICA'])
    
    contador = 0 
    for x in data.values:
        tuplas = tuple(x) 
        try:
            cursor = db.cursor()
            query = "insert into natureza_despesa(id, id_grupo_despesa, nome_grupo_despesa, id_elemento_despesa, nome_elemento_despesa, id_modalidade_despesa, nome_modalidade_despesa)  values (null,%s,%s,%s,%s,%s,%s);"
            cursor.execute(query, tuplas)
            db.commit()
            contador = contador + 1
        except Exception as e:
            print("erro",e)
            pass

    print(contador, "natureza inserted")


def extract_funcao(name_file,d_o):
    db = get_connect()
    data = pd.read_csv(name_file, encoding='utf8', sep=";")
    if d_o=="d":
        data = data[["CODIGO FUNCAO", "NOME FUNCAO", "CODIGO SUBFUCAO", "NOME SUBFUNCAO"]]
        data = data.drop_duplicates(subset=["CODIGO FUNCAO","CODIGO SUBFUCAO"])
    else:
        data = data[["CODIGO FUNCAO", "NOME FUNCAO", "CODIGO SUBFUNCAO", "NOME SUBFUNCAO"]]
        data = data.drop_duplicates(subset=["CODIGO FUNCAO","CODIGO SUBFUNCAO"])

    contador = 0 
    for x in data.values:
        tuplas = tuple(x)
        try:
            cursor = db.cursor()
            query = "insert into funcao(id, id_funcao, nome_funcao, id_subfuncao, nome_subfuncao) VALUES (null, %s, %s, %s, %s);"
            cursor.execute(query, tuplas)
            db.commit()
            contador = contador + 1
        except Exception as e:
            print("erro",e)
            # print("tuplas",tuplas)
            pass
    print(contador, "funcao inserted")


def extract_programa(name_file):
    db = get_connect()

    data = pd.read_csv(name_file, encoding='utf8', sep=";")

    data = data[["CODIGO PROGRAMA ORCAMENTARIO", "NOME PROGRAMA ORCAMENTARIO", "CODIGO ACAO", "NOME ACAO"]]
    
    data = data.drop_duplicates(subset=["CODIGO PROGRAMA ORCAMENTARIO","CODIGO ACAO"])
    
    contador = 0 
    for x in data.values:
        tuplas = tuple(x)
        try:
            cursor = db.cursor()
            query = "insert into programa(id, id_programa, nome_programa, id_acao, nome_acao) VALUES (null, %s, %s, %s, %s);"
            cursor.execute(query, tuplas)
            db.commit()
            contador = contador + 1
        except Exception as e:
            print("erro",e)
            # print("tuplas",tuplas)
            pass

    print(contador, "programa inserted")


def removerCaracteresEspeciais(name_file,j):
    """
    Método para remover caracteres especiais do texto
    """
    file_name = "./despesas/2018"+j+"_sem_Despesas.csv"
    list_lines = open(name_file).readlines()
    file = open(file_name, "w")
    for line in list_lines:
        line = unidecode.unidecode(line)
        line = line.upper()
        file.write(line)
    return file_name



def convert2float(arquivo,j,d_o):
    if d_o =="d":
        file_name="./despesa_final/2018"+str(j)+"_Despesas.csv"
        file = open("./despesa_final/2018"+str(j)+"_Despesas.csv", "w")
    else:
        file_name="./orcamento_final/2018"+str(j)+"_Orcamentos.csv"
        file = open("./orcamento_final/2018"+str(j)+"_Orcamentos.csv", "w")
    
    
    with open(arquivo) as csvfile:
        readCSV = csv.reader(csvfile, delimiter=';')    
        for row in readCSV:
            if d_o =="d":
                row[27] = row[27].replace(",",".")
                row[28] = row[28].replace(",",".")
                row[29] = row[29].replace(",",".")
                row[30] = row[30].replace(",",".")
                row[31] = row[31].replace(",",".")
                row[32] = row[32].replace(",",".")
                string = str(row).replace("'",'"')
                replaced = string.replace("[","").replace("]","").replace(",",";").replace(' "','"')
                file.write(replaced+'\n')
            else:
                try:
                    row[21] = row[21].replace(",",".")
                    row[22] = row[22].replace(",",".")
                    row[23] = row[23].replace(",",".")
                    string = str(row).replace("'",'"')
                    replaced = string.replace("[","").replace("]","").replace(",",";").replace(' "','"')
                    file.write(replaced+'\n')
                except Exception as e:
                    print(e)
                    pass
    return file_name

def insert_liquidado_fato(despesa):
    """
    extrair valores liquidados pelos 
    codigos (VALOR LIQUIDADO (R$)) 
    """
    db = get_connect()
    despesas = pd.read_csv(despesa, encoding='utf8', sep=";")
    
    
    splited= despesa.split("/")
    splited = splited[2].split("_")
    data_despesa = splited[0]
  
    df = despesas.groupby([
        "CODIGO ORGAO SUPERIOR", "CODIGO ORGAO SUBORDINADO","CODIGO UNIDADE ORCAMENTARIA",
        "CODIGO FUNCAO","CODIGO SUBFUCAO",
        "CODIGO PROGRAMA ORCAMENTARIO","CODIGO ACAO",
        "CODIGO GRUPO DE DESPESA","CODIGO ELEMENTO DE DESPESA","CODIGO MODALIDADE DA DESPESA"],as_index=False).agg({"VALOR LIQUIDADO (R$)": "sum"})

    for x in df.values:
        # print(tupla)
        tupla = tuple(x)
        # print(tupla)
        instituicao_id=0
        funcao_id=0
        programa_id=0
        natureza_despesa_id=0
        periodo_id=0
        # buscando fk instituição para inserir na tabela fato 
        try:
            cursor = db.cursor()
            query_instituicao = "select id from instituicao where id_orgao=%s and id_orgao_subordinado=%s and id_unidade_gestora=%s"
            cursor.execute(query_instituicao,(str(tupla[0]),str(tupla[1]),str(tupla[2])))
            records = cursor.fetchall()
            for record in records:
                instituicao_id = record[0]
        except Exception as e:
            # print("instituicao",e)
            pass
        

        # buscando fk funcao para inserir na tabela fato
        try:
            cursor = db.cursor()
            query_funcao = "select id from funcao where id_funcao=%s and id_subfuncao=%s"
            cursor.execute(query_funcao,(str(tupla[3]),str(tupla[4])))
            records = cursor.fetchall()
            for record in records:
                funcao_id = record[0]
        except Exception as e:
            # print("funcao",e)
            pass


        # buscando fk programa para inserir na tabela fato
        try:
            cursor = db.cursor()
            query_programa = "select id from programa where id_programa=%s and id_acao=%s"
            cursor.execute(query_programa,(str(tupla[5]),str(tupla[6])))
            records = cursor.fetchall()
            for record in records:
                programa_id = record[0]
        except Exception as e:
            # print("programa",e)
            pass

        
        # buscando fk natureza das despesas para inserir na tabela fato
        try:
            cursor = db.cursor()
            query_natureza = "select id from natureza_despesa where id_grupo_despesa=%s and id_elemento_despesa=%s and id_modalidade_despesa=%s"
            cursor.execute(query_natureza,(str(tupla[7]),str(tupla[8]),str(tupla[9])))
            records = cursor.fetchall()
            for record in records:
                natureza_despesa_id = record[0]
        except Exception as e:
            # print("natureza",e)
            pass
        

        # buscando fk data das despesas para inserir na tabela fato
        try:
            cursor = db.cursor()
            query_natureza = "select id from lancamento where data_lancamento="+data_despesa 
            cursor.execute(query_natureza)
            records = cursor.fetchall()
            for record in records:
                periodo_id = record[0]
        except Exception as e:
            # print("natureza",e)
            pass

        if not instituicao_id==0:
            cursor = db.cursor()
            try:
                query = "insert into fato_despesa(instituicao_id,programa_id,funcao_id,natureza_despesa_id,periodo_id,valor_liquidado,valor_orcado) VALUES (%s, %s, %s, %s,%s,%s,0);"
                values = (instituicao_id,programa_id,funcao_id,natureza_despesa_id,periodo_id,tupla[-1])
                cursor.execute(query,values)
                db.commit()
            except Exception as e:
                print(e)

                try:
                    query = "update fato_despesa set valor_liquidado=%s where instituicao_id=%s and programa_id=%s and funcao_id=%s and natureza_despesa_id=%s and periodo_id=%s;"
                    values = (tupla[-1],instituicao_id,programa_id,funcao_id,natureza_despesa_id,periodo_id)
                    cursor.execute(query,values)
                    db.commit()
                except Exception as a:
                    pass
                pass

def insert_orcado_fato(despesa):
    """
    extrair valores liquidados pelos 
    codigos (VALOR LIQUIDADO (R$)) 
    """
    db = get_connect()
    despesas = pd.read_csv(despesa, encoding='utf8', sep=";")
    
    
    splited= despesa.split("/")
    splited = splited[2].split("_")
    data_despesa = splited[0]
  
    df = despesas.groupby([
        "CODIGO ORGAO SUPERIOR", "CODIGO ORGAO SUBORDINADO","CODIGO UNIDADE ORCAMENTARIA",
        "CODIGO FUNCAO","CODIGO SUBFUNCAO",
        "CODIGO PROGRAMA ORCAMENTARIO","CODIGO ACAO",
        "CODIGO GRUPO DE DESPESA","CODIGO ELEMENTO DE DESPESA","CODIGO CATEGORIA ECONOMICA"],as_index=False).agg({"ORCAMENTO INICIAL (R$)": "sum"})

    for x in df.values:
        tupla = tuple(x)
        instituicao_id=0
        funcao_id=0
        programa_id=0
        natureza_despesa_id=0
        periodo_id=0
        # buscando fk instituição para inserir na tabela fato 
        try:
            cursor = db.cursor()
            query_instituicao = "select id from instituicao where id_orgao=%s and id_orgao_subordinado=%s and id_unidade_gestora=%s"
            cursor.execute(query_instituicao,(str(tupla[0]),str(tupla[1]),str(tupla[2])))
            records = cursor.fetchall()
            for record in records:
                instituicao_id = record[0]
        except Exception as e:
            # print("instituicao",e)
            pass
        

        # buscando fk funcao para inserir na tabela fato
        try:
            cursor = db.cursor()
            query_funcao = "select id from funcao where id_funcao=%s and id_subfuncao=%s"
            cursor.execute(query_funcao,(str(tupla[3]),str(tupla[4])))
            records = cursor.fetchall()
            for record in records:
                funcao_id = record[0]
        except Exception as e:
            # print("funcao",e)
            pass


        # buscando fk programa para inserir na tabela fato
        try:
            cursor = db.cursor()
            query_programa = "select id from programa where id_programa=%s and id_acao=%s"
            cursor.execute(query_programa,(str(tupla[5]),str(tupla[6])))
            records = cursor.fetchall()
            for record in records:
                programa_id = record[0]
        except Exception as e:
            # print("programa",e)
            pass

        
        # buscando fk natureza das despesas para inserir na tabela fato
        try:
            cursor = db.cursor()
            query_natureza = "select id from natureza_despesa where id_grupo_despesa=%s and id_elemento_despesa=%s and id_modalidade_despesa=%s"
            cursor.execute(query_natureza,(str(tupla[7]),str(tupla[8]),str(tupla[9])))
            records = cursor.fetchall()
            for record in records:
                natureza_despesa_id = record[0]
        except Exception as e:
            # print("natureza",e)
            pass
        

        # buscando fk data das despesas para inserir na tabela fato
        try:
            cursor = db.cursor()
            query_natureza = "select id from lancamento where data_lancamento="+data_despesa 
            cursor.execute(query_natureza)
            records = cursor.fetchall()
            for record in records:
                periodo_id = record[0]
        except Exception as e:
            # print("natureza",e)
            pass

        if not instituicao_id==0:
            cursor = db.cursor()
            try:
                query = "insert into fato_despesa(instituicao_id,programa_id,funcao_id,natureza_despesa_id,periodo_id,valor_liquidado,valor_orcado) VALUES (%s, %s, %s, %s,%s,0,%s);"
                values = (instituicao_id,programa_id,funcao_id,natureza_despesa_id,periodo_id,tupla[-1])
                cursor.execute(query,values)
                db.commit()
            except Exception as e:
                try:
                    query = "update fato_despesa set valor_orcado=%s where instituicao_id=%s and programa_id=%s and funcao_id=%s and natureza_despesa_id=%s and periodo_id=%s;"
                    values = (tupla[-1],instituicao_id,programa_id,funcao_id,natureza_despesa_id,periodo_id)
                    cursor.execute(query,values)
                    db.commit()
                except Exception as a:
                    pass
                pass

if __name__ == '__main__':
    # despesas liquidadas
    for i in range(12):
        j = i+1
        if j < 10:
            input_name_file = "./despesas/20180"+str(j)+"_Despesas.csv"
            name_new_file = removerCaracteresEspeciais(input_name_file,"0"+str(j))
            new_file = convert2float(name_new_file,"0"+str(j),"d")
        else:
            input_name_file = "./despesas/2018"+str(j)+"_Despesas.csv"
            name_new_file = removerCaracteresEspeciais(input_name_file,str(j))
            new_file = convert2float(name_new_file,str(j),"d")

        extract_natureza_despesa(name_new_file,"d")
        extract_programa(name_new_file)
        extract_funcao(name_new_file,"d")
        extract_instituicao(name_new_file)
        extract_lancamento(name_new_file)
    
    # despesas orcadas
    for i in range(12):
        j = i+1
        if j < 10:
            input_name_file = "./orcamentos/20180"+str(j)+"_Orcamento.csv"
            # name_new_file = removerCaracteresEspeciais(input_name_file)
            name_new_file = convert2float(input_name_file,"0"+str(j),"o")
        else:
            input_name_file = "./orcamentos/2018"+str(j)+"_Orcamento.csv"
            # name_new_file = removerCaracteresEspeciais(input_name_file)
            name_new_file = convert2float(input_name_file,str(j),"o")

        extract_lancamento(name_new_file)
        extract_natureza_despesa(name_new_file,"o")
        extract_programa(name_new_file)
        extract_funcao(name_new_file,"o")
        extract_instituicao(name_new_file)
        extract_lancamento(name_new_file)
    
    # valores de ./despesas
    for i in range(12):
        j = i+1
        print("mes",j)
        if j < 10:
            insert_liquidado_fato("./despesa_final/20180"+str(j)+"_Despesas.csv")
        else:
            insert_liquidado_fato("./despesa_final/2018"+str(j)+"_Despesas.csv")
    
    # valores de ./orcamento
    for i in range(12):
        j = i+1
        print("mes",j)
        if j < 10:
            insert_orcado_fato("./orcamento_final/20180"+str(j)+"_Orcamentos.csv")
        else:
            insert_orcado_fato("./orcamento_final/2018"+str(j)+"_Orcamentos.csv")