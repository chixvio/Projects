import pandas as pd
import re


def unpackdf(df, target):
    try:
        value = df[target].dropna().iloc[0]
    except:
        value = ""
    return value


def dwobjects(file):
    print("[+] Creating variables...")
    df = pd.read_excel(file, sheet_name="BusMatrix", engine='openpyxl')
    df2 = pd.read_excel(file, sheet_name="DataDictionary", engine='openpyxl')

    df = df.set_index(df.columns[0])
    dimensions  = list(df.columns[1:])
    facts       = list(df.index)


    class dw:
        def __init__(self, name, type, pk, links, meta, sourcepk, incremental):
            self.name           = name
            self.type           = type
            self.pk             = pk
            self.links          = links
            self.meta           = meta
            self.sourcepk       = sourcepk
            self.incremental    = incremental


    objectList = []   
    for i in dimensions:
        name            = i.replace('Dim','')
        links           = list(df[[i]].dropna().index)
        data            = df2[df2["DIMENSION"] == i]
        meta            = data[['COLUMN_NAME','DATA_TYPE','IS_NULLABLE']].values.tolist()

        objectList.append(dw(name, 'dim', f"{name}Key", links, meta, unpackdf(data,'PRIMARY_KEY'), unpackdf(data,'INCREMENTAL')))

    for i in facts:
        name            = i.replace('Fact','')
        links           = list(df.filter(items=[i], axis=0).dropna(axis=1).columns)
        data            = df2[df2["DIMENSION"] == i]
        meta            = data[['COLUMN_NAME','DATA_TYPE','IS_NULLABLE']].values.tolist()

        objectList.append(dw(name, 'fct', f"{name}Key", links, meta, unpackdf(data,'PRIMARY_KEY'), unpackdf(data,'INCREMENTAL')))

    return objectList


def etl_view(object_list):
    print("[+] Creating ETL Views...")
    Query = ""
    for dw in object_list:
        columns = ""
        for col in dw.meta:
            columns	+= f"\n\t[{col[0]}],"
        Query 		+= f"CREATE VIEW [{dw.type}].[{dw.name}]\nAS\nSELECT{columns[:-1]}\nFROM [stg].[{dw.name}]\nGO\n\n"
    return	Query


def aas_view(object_list):
    print("[+] Creating Presentation Views...")
    Query = ""
    for dw in object_list:
        columns = ""
        for col in dw.meta:
            newcol = re.sub(r"(\w)([A-Z])",r'\1 \2', col[0])
            columns	+= f"\n\t,[{col[0]}] AS [{newcol}]"
        Query 		+= f"CREATE VIEW [aas].[{dw.name}]\nAS\nSELECT\n\t [{dw.pk}]{columns}\nFROM [{dw.type}].[{dw.name}]\nGO\n\n"
        # sample = re.sub(r'\B([A-Z])', r' \1','[DemoTableNumberOne]')
    return	Query


def etl_proc(object_list):
    print("[+] Creating ETL Stored Procedures...")
    Query = ""
    for dw in object_list:
        target, source, target_source, column = ['' for i in range(4)]

        for col in dw.meta:
            column			+= f"\n\t[{col[0]}],"
            target			+= f"\n\tTarget.[{col[0]}],"
            source			+= f"\n\tSource.[{col[0]}],"
            target_source 	+= f"\n\tTarget.[{col[0]}] = Source.[{col[0]}],"
        Query += f"""
    CREATE PROC [{dw.type}].[populate_{dw.name}]
    AS
    BEGIN
    MERGE INTO [{dw.type}].[{dw.name}] TARGET
    USING [{dw.type}].[create_{dw.name}] SOURCE
        ON TARGET.[{dw.sourcepk}] = SOURCE.[{dw.sourcepk}]
    WHEN MATCHED
        AND TARGET.[{dw.incremental}] <> SOURCE.[{dw.incremental}] 
        THEN UPDATE {target_source[:-1]}
    WHEN NOT MATCHED THEN
        INSERT into Employee({target[:-1]}
            )
        VALUES({source[:-1]}
            );
    END
    GO
    """
    return Query


def create_table(object_list):
    print("[+] Creating dim & Fact Tables...")
    Query = ""
    for dw in object_list:
        identity 	= f"[{dw.pk}] [int] IDENTITY(1,1) NOT NULL,"
        columns = ""
        for col in dw.meta:
            col[2] = col[2].lower().replace('yes','null').replace('no','not null')
            columns	+= f"\n\t[{col[0]}] {col[1]} {col[2]},"

        constraint	 = f"CONSTRAINT [PK_{dw.name}] PRIMARY KEY CLUSTERED ([{dw.pk}] ASC))"
        Query		+= f"CREATE TABLE [{dw.type}].[{dw.name}](\n\t{identity}{columns}\n{constraint}\nGO\n\n"

    # Add foreign key constraint for fact on dim
    return Query


def foreign_keys(object_list):
    #fk_query = f"ALTER TABLE [{schema}].[{tableName}]  WITH CHECK ADD  CONSTRAINT [{tbc}] FOREIGN KEY([{tbc}])\nREFERENCES [{schema}].[{tbc}] ([{tbc}])"
    pass


def run_all():
    print("\n********** Created by @chixvio **********\n")
    file = input("Please enter the location of your requirements.xlsm file :")
    response = input(f"You selected {file} \n is that correct? (y/n)").lower()
    if response == 'y' or response == "yes":
        pass
    else:
        exit() 
       
    output = input("Please enter a destination for you sql file :")
    response = input(f"You selected {output} \n is that correct? (y/n)").lower()
    if response == 'y' or response == "yes":
        pass
    else:
        exit()  
    sql = ""
    object_list =   dwobjects(file)
    sql +=  etl_view(object_list)
    sql +=  etl_proc(object_list)
    sql +=  create_table(object_list)
    sql +=  aas_view(object_list)

    print(f"[+] Writing sql file... {output}")
    with open(output, 'w') as f:
        f.write(sql)
    print("[\1] Completed.") 


    print("\n\n----------------------------------")
    print("        Upcoming features")
    print("----------------------------------")
    print("[*] Auto Foreign Key constraints")
    print("[*] Run from cmd/Terminal")
    print("[*] Read meta data at source")
    print("[*] Update existing objects")
    print("[*] Data factory / Azure additions")
    return sql

run_all()