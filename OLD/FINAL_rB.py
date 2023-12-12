import requests
import sqlite3
from datetime import datetime
import pandas as pd
format="%Y-%m-%d" #FORMATO DE TIEMPO

def crearBaseDatos():
    try:
        conn=sqlite3.connect(f"TickerBaseDatos.db")
        c = conn.cursor()
        c.execute (f''' CREATE TABLE TickerGuardados ( 
                                                Ticker TEXT PRIMARY KEY, FechaInicio TEXT, FechaFinal TEXT
                                                )''')
        conn.commit()
        conn.close()
    except sqlite3.OperationalError:
        return None
crearBaseDatos()             #Crea la Base Datos Inicial 

def actualizarTickerBD (datosTicker,nombreTabla):
        conn=sqlite3.connect(f"TickerBaseDatos.db")
        c = conn.cursor()
        c.execute (f'''CREATE TABLE {nombreTabla}( 
                        Fecha TEXT, VolumenOperado REAL, PrecioPromedioPorVolumen REAL, PrecioApertura REAL, PrecioCierre REAL, 
                        PrecioMásAlto REAL, PrecioMásBajo REAL, NúmeroDeTransacciones REAL
                        )    ''')

        fechaData = []
        fechaDataString=[]
        for i in range(0,len(datosTicker)):
            fechaData.append(datetime.fromtimestamp(datosTicker[i]['t']/1000.0).date())
            fechaDataString.append(fechaData[i].strftime(format)) #FECHA EN STRING
            c.execute (f'''INSERT INTO {nombreTabla} (Fecha, VolumenOperado, PrecioPromedioPorVolumen, PrecioApertura, PrecioCierre, PrecioMásAlto,
                                                PrecioMásBajo, NúmeroDeTransacciones) 
                                VALUES ('{fechaDataString[i]}',{datosTicker[i]['v']},{datosTicker[i]['vw']},{datosTicker[i]['o']},
                                        {datosTicker[i]['c']},{datosTicker[i]['h']},{datosTicker[i]['l']},
                                        {datosTicker[i]['n']});
                                ''') #cotizaciones['results'][i]['t']
        conn.commit()
        conn.close()
        print("\tDatos guardados correctamente")

def actualizarListaDatos (ticker, fechaInicio, fechaFinal):
        conn=sqlite3.connect(f"TickerBaseDatos.db")
        c = conn.cursor()
        c.execute (f'''INSERT INTO TickerGuardados (Ticker, FechaInicio, FechaFinal) 
                                       VALUES ('{ticker}','{fechaInicio}','{fechaFinal}');
                              ''')  
        conn.commit()
        conn.close()

def validarFechas():
    while True:
        while True:
                    try:
                        fechaInicioStr=input('\tIngrese Fecha de Inicio (YYYY-MM-DD):')        
                        fechaInicio=datetime.strptime(fechaInicioStr,format).date() 
                    except ValueError:
                            print("\tLa fecha inicial ingresada es inválida!\n")
                    else:
                        break
        while True:
                    try:
                        fechaFinalStr=input('\tIngrese Fecha de Final (YYYY-MM-DD):')
                        fechaFinal=datetime.strptime(fechaFinalStr,format).date()
                    except ValueError:
                            print("\tLa fecha final ingresada es inválida!\n")
                    else:
                        break

        if fechaInicio>fechaFinal:
                    print("\n\tERROR!\nLa fecha inicial es posterior a la fecha final\n") 
        elif fechaInicio>datetime.now().date() or fechaFinal>datetime.now().date():
                    print(f"\n\tERROR!\nUna de las fechas ingresadas es posterior a la fecha actual {datetime.now().date()}\n")
        else: 
             break
    return fechaInicioStr, fechaFinalStr          

def validarTicker():
        while True:
            ticker=input("\tIngrese el ticker: \t").upper() 
            tickerData=requests.get(f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/2023-01-01/2023-01-10?apiKey=_E06pfStxn1XpmSEBG7HUwEeV7029dfW")
            tickerDataDict=tickerData.json()      
            if tickerDataDict['queryCount']!=0: 
                break
            else: 
                print("\tEl ticker ingresado es inválido\n")
                continue
        return ticker

def solicitarDatosTicker(ticker,fechaInicio,fechaFinal):
        fechaInicioSin=fechaInicio.replace("-","")
        fechaFinalSin=fechaFinal.replace("-","")
        nombreTabla=ticker+"_"+fechaInicioSin+"_"+fechaFinalSin
        tickerData=requests.get(f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{fechaInicio}/{fechaFinal}?apiKey=_E06pfStxn1XpmSEBG7HUwEeV7029dfW")
        tickerDataDict=tickerData.json()
        if tickerDataDict['status']=='OK':
                print("\tSolicitando Datos...")
                actualizarListaDatos(ticker, fechaInicio, fechaFinal)                        
                actualizarTickerBD(tickerDataDict['results'],nombreTabla)
        elif tickerDataDict['status']=='NOT_AUTHORIZED':
                print("\n\tSu plan no incluye este período de datos. Intente a partir del 2022.\n")
                actualizacionDatos()
        
def actualizacionDatos ():
        print("\nACTUALIZACIÓN DE DATOS\n")
        ticker=validarTicker()
        fechaInicio, fechaFinal = validarFechas()
        solicitarDatosTicker(ticker,fechaInicio,fechaFinal)
        #print("\tDatos guardados correctamente.")

def visualizacionDatosAlmacenados():
        print("\nRESUMEN DE DATOS ALMACENADOS EN BASE DATOS")
        conn=sqlite3.connect('TickerBaseDatos.db')
        df=pd.read_sql(con=conn,sql="SELECT * FROM TickerGuardados")
        print("\nLos datos guardados en la base de datos son:\n")
        print(f'\tTicker\t-\tFecha Inicio\t<->\tFecha Final')
        for i in range (0,len(df.Ticker)):
                print(f'\t{df.Ticker[i]}\t-\t{df.FechaInicio[i]}\t<->\t{df.FechaFinal[i]}')
        conn.close()  

def menuVisualizacionDatos():
       while True: #SELECCIÓN DE MENÚ
            print("\nVISUALIZACIÓN DE DATOS:\n")
            print("SELECCIONE UNA OPCIÓN:\n 1. RESUMEN \n 2. GRÁFICO DE TICKER\n 3. VOLVER")
            opcion=input("\n\tINGRESE OPCIÓN: ")   
            if opcion == "1":
                        visualizacionDatosAlmacenados()
                        break
            elif opcion=="2": 
                        #graficarTicker()
                        print("FUNCION DE GRAFICAR TICKER AUN NO CREADA")
                        break
            elif opcion=="3":   
                        menuInicial()
                        break
            else:   
                print("\tOPCIÓN INVÁLIDA")

def menuInicial():
        while True: #SELECCIÓN DE MENÚ
                print("\nINFORMACIÓN DE TICKER")
                print("\nSELECCIONE UNA OPCIÓN:\n 1. ACTUALIZACIÓN DE DATOS\n 2. VISUALIZACIÓN DE DATOS\n 3. SALIR")
                opcion=input("\n\tINGRESE OPCIÓN: ")   
                if opcion == "1":
                                actualizacionDatos()
                                break
                elif opcion=="2": 
                                menuVisualizacionDatos()
                                break
                elif opcion=="3":   
                                print("\tADIOS")
                                break
                else:   
                        print("\tOPCIÓN INVÁLIDA")

menuInicial()