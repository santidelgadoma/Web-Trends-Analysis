"""
Santiago Delgado
30/04/2024
Codigo para obtener trends de palabras en google de distintos paises con Pytrends

"""

from pytrends.request import TrendReq #importamos la biblioteca TrendReq 
import pandas as pd #importamos pandas
import time #importamos las funciones de time

def buscar_datos(item,palabra,tabla=None): #La funcion recibe el nombre del pais, la palabra y un DataFrame que en caso de no existir es declarado como None
    format_date = "2004-01-01 2024-4-29"  #Se declara el rango de la busqueda    
    pytrends.build_payload(kw_list=palabra, geo=item,timeframe=[format_date]) #crea el payload con el que se hara el request
    info = pytrends.interest_over_time() #busca los trends a traves del tiempo del payload especificado y los guarda en un DataFrame
    if tabla is not None: #Si ya hay una tabla insertamos los datos nuevos como una columna
        try:          
            tabla.insert(len(tabla.columns),palabra,info.iloc[:,0],allow_duplicates=True) #el primer parametro indica que la columna se agrega despues de la ultima columna
                                                                                          #el segundo parametro es el nombre de la columna en este caso la palabra
                                                                                          #el tercer parametro es la primera columna del DataFrame obtenido en el request
                                                                                          #el cuarto parametro admite duplicados en la matriz
        except Exception as e: #Si no es exitoso el insert puede ser porque no encontro información entonces le agregamos una columna de puros ceros
            print(e)
            filler = [0]*244
            tabla.insert(len(tabla.columns),palabra,filler,allow_duplicates=True)
    else: #Si no hay una tabla creada iniciamos un dataframe con la informacion de la primera palabra
        try:
            tabla = pd.DataFrame(data=info.iloc[:,0],index = info.index,columns=palabra) #el primer parametro es la información de la primera columna del DataFrame
                                                                                         #el segundo parametro son los indices que son reutilizados del DataFrame obtenido
                                                                                         #el tercer parametro es la palabra que sirve de nombre de la columna
        except Exception as e: #Si hay algun error se imprime a consola el error
            print(e)
    return tabla #la funcion regresa la tabla creada o actualizada


pytrends = TrendReq(hl="en-US") #inicia la conexion con el servidor

pais = ['DK','FI','FR','DE','IN','IT','JP','KR','MX','NL','PT','ES','SE','CH','TW','TR','GB','US'] #declara la lista de paises a investigar
palabras = ['war','conflict','hostilities','revolution','insurrection','uprising','revolt','coup','geopolitical']#declara las palabras a investigar

# Filtramos para excluir palabras que contengan "movie" o "book"
palabras_filtradas = [palabra for palabra in palabras if "movie" not in palabra and "book" not in palabra]

print(palabras_filtradas)

for k in range(len(pais)): #itera sobre el arreglo de paises
    for i, palabra in enumerate(palabras_filtradas): #itera sobre el arreglo de palabras filtradas
        print("Starting " + pais[k] + " " + palabra) #Imprime a consola el pais y palabra que va a investigar
        if i == 0:
            tabla = buscar_datos(pais[k], [palabra]) #llama a la funcion para buscar los datos y guarda la tabla obtenida
        else: #si ya hay una palabra en la tabla se envía la tabla obtenida para agregar la información del resto de las palabras
            tabla = buscar_datos(pais[k], [palabra], tabla)  #llama a la funcion bara agregar los datos de la palabra a la tabla
        time.sleep(5) #descansa unos segundos entre requests para evitar errores
    filename = f'{pais[k]}.csv' #cuando termina de investigar todas las palabras para un pais las escribe en un csv con el nombre del pais
    tabla.to_csv(filename, index=True)
