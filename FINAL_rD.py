import requests
import sqlite3
from datetime import datetime, timedelta
import pandas as pd
import matplotlib.pyplot as plt

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

def crearTablaTicker (datosTicker,nombreTabla):
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
                                ''')
        conn.commit()
        conn.close()
        print("\tDatos guardados correctamente")

def insertarEnTablaTicker(datosTicker,nombreTabla):
        conn=sqlite3.connect(f"TickerBaseDatos.db")
        c = conn.cursor()
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
                                ''') 
        conn.commit()
        conn.close()
        print("\tDatos guardados correctamente")

def insertarListaDatos (ticker, fechaInicio, fechaFinal):
        conn=sqlite3.connect(f"TickerBaseDatos.db")
        c = conn.cursor()
        c.execute (f'''INSERT INTO TickerGuardados (Ticker, FechaInicio, FechaFinal) 
                                       VALUES ('{ticker}','{fechaInicio}','{fechaFinal}');
                              ''')  
        conn.commit()
        conn.close()

def actualizarListaDatos(ticker, fechaInicio, fechaFinal):
        conn=sqlite3.connect(f"TickerBaseDatos.db")
        c = conn.cursor()
        c.execute (f'''UPDATE TickerGuardados
                        SET FechaInicio = '{fechaInicio}',
                            FechaFinal = '{fechaFinal}'
                        WHERE Ticker = '{ticker}'
                       ''')  
        conn.commit()
        conn.close()       

def borrarTabla(ticker):
        conn=sqlite3.connect(f"TickerBaseDatos.db")
        c = conn.cursor()
        c.execute (f'''DROP TABLE {ticker}
                              ''')  
        conn.commit()
        conn.close()   

def borrarRegistro(ticker):
        conn=sqlite3.connect(f"TickerBaseDatos.db")
        c = conn.cursor()
        c.execute (f'''DELETE FROM TickerGuardados
                       WHERE Ticker = '{ticker}';
                              ''')  
        conn.commit()
        conn.close() 

def validarFechasIngresadas():
        while True:
                while True:             #Valida Fecha Inicial
                        try:
                                fechaInicioStr=input('\tIngrese Fecha de Inicio (YYYY-MM-DD):')        
                                fechaInicio=datetime.strptime(fechaInicioStr,format).date() 
                        except ValueError:
                                print("\tLa fecha inicial ingresada es inválida!\n")
                        else:
                                break
                while True:             #Valida Fecha Final
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
        return fechaInicio, fechaFinal         

def validarTicker():
    con=sqlite3.connect('TickerBaseDatos.db')
    df=pd.read_sql(con=con,sql="SELECT * FROM TickerGuardados")
    con.close()
    listaTicker=list(df.Ticker)
    while True:
            try:
                ticker=input("\tIngrese el ticker: ").upper()  
                listaTicker.index(ticker) #PRIMERO VERIFICA SI EL TICKER EXISTE EN NUESTRA BASE DE DATOS
            except ValueError:             #SI NO EXISTE, ENTONCES LA VA A VALIDAR HACIENDO UN REQUEST
                tickerData=requests.get(f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/2023-01-01/2023-01-10?apiKey=_E06pfStxn1XpmSEBG7HUwEeV7029dfW")
                tickerDataDict=tickerData.json()      
                if tickerDataDict['queryCount']!=0:
                    return listaTicker, ticker
                else: 
                    print("\tEl ticker ingresado es inválido\n") #SI EL TICKER NO EXISTE DEBERÍA IMPRIMIR UN ERROR.
                continue
            else:
                 break
    return listaTicker, ticker
     
def solicitarDatosTicker(ticker,fechaInicio,fechaFinal):
        fechaInicioStr=datetime.strftime(fechaInicio,format)
        fechaFinalStr=datetime.strftime(fechaFinal,format)
        tickerData=requests.get(f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{fechaInicioStr}/{fechaFinalStr}?apiKey=_E06pfStxn1XpmSEBG7HUwEeV7029dfW")
        tickerDataDict=tickerData.json()
        if tickerDataDict['status']=='OK' and tickerData.status_code == 200:
                        print("\tSolicitando Datos...")
                        return tickerDataDict['results']                       
        elif tickerDataDict['status']=='NOT_AUTHORIZED':
                        print("\n\tSu plan no incluye este período de datos. Intente a partir del 2022.\n")
                        validarFechasIngresadas()
        elif tickerData.status_code == 403:
                        print("ERROR! 403 FORBIDDEN")
        elif tickerData.status_code == 404:
                        print("ERROR! 404 NOT FOUND")

def verificacionDatos(ticker, listaTicker,fechaInicio, fechaFinal):
      
        try:    #SI EL TICKER ES NUEVO Y NO EXISTE EN NUESTRA BASE DE DATOS
                indiceTicker=listaTicker.index(ticker)
        except ValueError: #SI EL TICKER EXISTE EN NUESTRA BASE DE DATOS, VERIFICAMOS LAS FECHAS 
                DatosTicker=solicitarDatosTicker(ticker,fechaInicio,fechaFinal)
                crearTablaTicker(DatosTicker,ticker)
                insertarListaDatos(ticker, fechaInicio, fechaFinal)                
        else:                
                con=sqlite3.connect('TickerBaseDatos.db')
                df=pd.read_sql(con=con,sql="SELECT * FROM TickerGuardados")
                con.close()
                listaTicker=list(df.Ticker)
                fechaIBD=datetime.strptime(df.FechaInicio[indiceTicker],format).date() #Fecha Inicial Base de Datos
                fechaFBD=datetime.strptime(df.FechaFinal[indiceTicker],format).date() #Fecha final Base Datos
                DeltaDia=timedelta(1)

                if fechaInicio<fechaIBD and fechaIBD<fechaFinal<fechaFBD:
                        fechaIS = fechaInicio
                        fechaFS = (fechaIBD-DeltaDia)
                        DatosTicker=solicitarDatosTicker(ticker,fechaIS,fechaFS)
                        insertarEnTablaTicker(DatosTicker,ticker) #INSERTAR EN LA TABLA
                        actualizarListaDatos(ticker,fechaIS,fechaFBD)
                        #ordena la tabla por fecha
                elif fechaIBD<fechaInicio<fechaFBD and fechaFBD<fechaFinal:
                        fechaIS = fechaFBD+DeltaDia
                        fechaFS = fechaFinal
                        DatosTicker=solicitarDatosTicker(ticker,fechaIS,fechaFS)
                        insertarEnTablaTicker(DatosTicker,ticker) #INSERTAR EN LA TABLA
                        actualizarListaDatos(ticker,fechaIBD,fechaFinal)
                        #ordena la tabla
                elif fechaInicio<fechaIBD and fechaFinal>fechaFBD:
                        fechaIS=fechaInicio
                        fechaFS=fechaFinal
                        borrarTabla(ticker)
                        borrarRegistro(ticker)
                        DatosTicker=solicitarDatosTicker(ticker,fechaIS,fechaFS)
                        crearTablaTicker(DatosTicker,ticker)
                        insertarListaDatos(ticker, fechaInicio, fechaFinal)   
                elif fechaInicio>fechaIBD and fechaFinal<fechaFBD:
                        print("\tEsos datos están disponibles en nuestra base de datos")

def actualizacionDatos ():
        print("\nACTUALIZACIÓN DE DATOS\n")
        listaTicker=[]
        listaTicker, ticker= validarTicker()
        fechaInicio, fechaFinal = validarFechasIngresadas()
        verificacionDatos(ticker, listaTicker,fechaInicio, fechaFinal)

def graficarTicker():
        con=sqlite3.connect('TickerBaseDatos.db')
        df=pd.read_sql(con=con,sql="SELECT * FROM TickerGuardados")
        con.close()
        listaTicker=list(df.Ticker)
        while True:
            try:
                tickerPrint=input("\n\tIngrese el ticker a imprimir: ").upper()  
                listaTicker.index(tickerPrint)
                break #PRIMERO VERIFICA SI EL TICKER EXISTE EN NUESTRA BASE DE DATOS
            except ValueError: 
                print("\tEl ticker ingresado no está en nuestra base datos, solicitelo desde ACTUALIZACIÓN DE DATOS.")
                continue
        con=sqlite3.connect('TickerBaseDatos.db')
        dPrint=pd.read_sql(con=con,sql=f"SELECT * FROM {tickerPrint}")
        con.close()

        print("\n\tLos parámetros disponibles son:")
        print("\n\t\t1.Volumen Operado\n\t\t2.Precio Promedio por Volumen\n\t\t3.Precio Apertura\n\t\t4.Precio Cierre\n\t\t5.Precio Más Alto\n\t\t6.Precio Más Bajo\n\t\t7.Número de Transacciones ")
        while True:
                parametro=input(f"\n\t\tQue parámetro desea imprimir?:")
                if parametro=="1":
                        parametroPrint="VolumenOperado"
                        break
                elif parametro=="2":
                        parametroPrint="PrecioPromedioPorVolumen"
                        break
                elif parametro=="3":
                        parametroPrint="PrecioApertura"
                        break
                elif parametro=="4":
                        parametroPrint="PrecioCierre"
                        break
                elif parametro=="5":
                        parametroPrint="PrecioMásAlto"
                        break
                elif parametro=="6":
                        parametroPrint="PrecioMásBajo"
                        break
                elif parametro=="7":    
                        parametroPrint="NúmeroDeTransacciones"
                        break
                else: 
                        print("\t\tERROR!! Parámetro inexistente")
                        continue
        dPrint.plot("Fecha",f"{parametroPrint}",kind="line",title=f"{tickerPrint}")
        plt.show()

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
                        graficarTicker()
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