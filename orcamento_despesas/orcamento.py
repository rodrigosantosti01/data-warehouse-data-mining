import csv
import tempfile
from locale import normalize

import pandas as pd
import mysql.connector as mysql
import numpy as np
import tempfile
import os
import unidecode

def removerCaracteresEspeciais(name_file):
    """
    Método para remover caracteres especiais do texto
    """
    file_name = "orcamentos/2018_OrcamentoDespesaSemCaracteres.csv"
    list_lines = open(name_file).readlines()
    file = open(file_name, "w")
    for line in list_lines:
        line = unidecode.unidecode(line)
        line = line.upper()
        file.write(line)
    return file_name


def split_dataframe(name_file):
    """
    Método para dividir o dataframe 
    orçamentário em 12 meses
    """
    path = os.getcwd()
    list_lines = open(name_file).readlines()
    divide = list(np.array_split(list_lines, 12))
    for i,f in enumerate(divide):
        data = str(1+i)
        file = open("orcamentos/2018"+("0"+str(1+i) if len(str(1+i))==1 else data)+"_Orcamento.csv", "w")
        file.write(divide[0][0])
        
        for d in divide[i]:
            d = d.replace("2018","2018/"+ ("0"+str(1+i) if len(str(1+i))==1 else data))
            if d != divide[0][0]:
                file.write(d)

if __name__ == '__main__':
    input_name_file = "orcamentos/2018_OrcamentoDespesa.csv"
    name_new_file = removerCaracteresEspeciais(input_name_file)
    split_dataframe(name_new_file)
    # extract_programa(name_new_file)
    