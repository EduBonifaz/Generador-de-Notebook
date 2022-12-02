import pandas as pd

Objetos = r'./Datum/Objects.csv'
Campos = r'./Datum/Fields.csv'

Input_Tablas = r'./Input_Tablas.xlsx'
Tablas_df = pd.read_excel(Input_Tablas, sheet_name='TABLA', dtype = 'object', usecols="A:B")
cutoff_value = pd.read_excel(Input_Tablas, sheet_name='TABLA', dtype = 'object', usecols="D").dropna()["cutoffDate"].tolist()
Objetos_df = pd.read_csv(Objetos ,sep=';',dtype = 'object')[["Physical name object","Description object","Partitions","physical path","Storage Zone","Storage Type"]]
Campos_df = pd.read_csv(Campos ,sep=';',dtype = 'object')[["Physical Name field","Key","Physical name object","Logical Format","Storage Zone"]]

for item,row in Tablas_df.iterrows():
	if row["TABLA MASTER"] == row["TABLA MASTER"]:
		CamposMaster = Campos_df[(Campos_df["Physical name object"]==row["TABLA MASTER"]) & (Campos_df["Storage Zone"]=="MASTERDATA")]
		TablaMaster_df = Objetos_df[(Objetos_df["Physical name object"]==row["TABLA MASTER"]) & (Objetos_df["Storage Zone"]=="MASTERDATA")]
		PathMaster = TablaMaster_df["physical path"].tolist()[0].replace(" ","")
		ParticionesMaster =TablaMaster_df["Partitions"].tolist()[0]
		ParticionesMaster_Path = ""
		cutoffMaster = ""
		cutoffMasterH = ""
		if ParticionesMaster == ParticionesMaster:
			ParticionesMaster = ParticionesMaster.replace(" ","").split(",")
			for ParticionMaster in ParticionesMaster:
				if "cutoff_date" in ParticionMaster:
					cutoff = ParticionMaster
					cutoffMasterH = f"{cutoff}="
					if len(cutoff_value)>0:
						cutoffMaster = f'+ \\"/{cutoff}={cutoff_value[0].strftime("%Y-%m-%d")}\\"'
					else:
						cutoffMaster = f'+ \\"/{cutoff}={cutoff}_value\\"'
					break
				ParticionesMaster_Path += f"/{ParticionMaster}={ParticionMaster}_value"
			CamposLlavesMaster = CamposMaster[(CamposMaster["Key"]=='true') & ~(CamposMaster["Physical Name field"].isin(ParticionesMaster))]
		else:
			CamposLlavesMaster = CamposMaster[(CamposMaster["Key"]=='true')]

		Notebook = r"""{
  "cells": [
    {
      "cell_type": "code",
      "execution_count": null,
      "metadata": {},
      "outputs": [],
      "source": [
        "from pyspark.context import SparkContext \n",
        "from pyspark.sql.session import SparkSession \n",
        "from pyspark.sql.functions import * \n",
        "from pyspark.sql.functions import desc \n",
        "from pyspark.sql.types import * \n",
        "from IPython.core.display import HTML\n",
        "display(HTML(\"<style>pre { white-space: pre !important; }</style>\"))"
      ]
    },
    {
	    "cell_type": "markdown",
	  	"metadata": {},
	  	"source": [
	  	"#### hdfs dfs -ls -h -d """+PathMaster+ParticionesMaster_Path+f"/{cutoffMasterH}*"+r""""
	  	]
    },
    {
      "cell_type": "code",
      "source": [
        "pathMaster=\""""+PathMaster+ParticionesMaster_Path+r'\"\n",'+r"""
        "dateRangeMaster=[pathMaster"""+cutoffMaster+r"""]\n",
        "masterDF=sqlContext.read.option(\"basepath\",pathMaster).parquet(*dateRangeMaster)"
      ],
      "metadata": {},
      "execution_count": null,
      "outputs": []
    }"""

		if row["TABLA RAW"] == row["TABLA RAW"]:
			CamposRaw = Campos_df[(Campos_df["Physical name object"]==row["TABLA RAW"]) & (Campos_df["Storage Zone"]=="RAWDATA")]
			TablaRaw_df = Objetos_df[(Objetos_df["Physical name object"]==row["TABLA RAW"]) & (Objetos_df["Storage Zone"]=="RAWDATA")]
			PathRaw = TablaRaw_df["physical path"].tolist()[0].replace(" ","")
			ParticionesRaw =TablaRaw_df["Partitions"].tolist()[0]
			ParticionesRaw_Path = ""
			cutoffRaw = ""
			cutoffRawH = ""
			Zona = TablaRaw_df["Storage Type"].tolist()[0].replace(" ","")
			if ParticionesRaw == ParticionesRaw:
				ParticionesRaw = ParticionesRaw.replace(" ","").split(",")
				for ParticionRaw in ParticionesRaw:
					if "cutoff_date" in ParticionRaw:
						cutoff = ParticionRaw
						if len(cutoff_value)>0:
							if Zona == "HDFS-Parquet":
								cutoffRaw = f'+ \\"/{cutoff}={cutoff_value[0].strftime("%Y-%m-%d")}\\"'
							else:
								cutoffRaw = f'+ \\"/{cutoff}={cutoff_value[0].strftime("%Y%m%d")}\\"'
						else:
							cutoffRaw = f'+ \\"/{cutoff}={cutoff}_value\\"'
						break
					ParticionesRaw_Path += f"/{ParticionRaw}={ParticionRaw}_value"
				CamposLlavesRaw = CamposRaw[(CamposRaw["Key"]=='true') & ~(CamposRaw["Physical Name field"].isin(ParticionesRaw))]
			else:
				CamposLlavesRaw = CamposRaw[(CamposRaw["Key"]=='true')]
			Campos = list(set(CamposLlavesRaw["Physical Name field"].tolist())&set(CamposLlavesRaw["Physical Name field"].tolist()))
			if len(Campos)==0:
				CM = CamposMaster[(CamposMaster["Key"]=='false')]["Physical Name field"].tolist()
				CR = CamposRaw[(CamposRaw["Key"]=='false')]["Physical Name field"].tolist()
				Campos = list(set(CM)&set(CR))
				if len(Campos)>=5:
					Campos = Campos[:5]
			

			Notebook += r""",
  {
    "cell_type": "code",
    "source": [
      "masterDF.select(\""""+('\\", \\"').join(Campos)+r"""\").show(15,truncate=False)"
    ],
    "metadata": {},
    "execution_count": null,
    "outputs": []
  }"""
			if Zona == "HDFS-Avro":

				Notebook += r""",
  {
    "cell_type": "code",
    "source": [
      "pathRaw=\""""+PathRaw+ParticionesRaw_Path+r'\"\n",'+r"""
      "dateRangeRaw=[pathRaw"""+cutoffRaw+r"""]\n",
      "rawDF=sqlContext.read.option(\"basepath\",pathRaw).format(\"avro\").load(*dateRangeRaw)"
    ],
    "metadata": {},
    "execution_count": null,
    "outputs": []
  }"""
			else:
 				Notebook += r""",
  {
    "cell_type": "code",
    "source": [
      "pathRaw=\""""+PathRaw+ParticionesRaw_Path+r'\"\n",'+r"""
      "dateRangeRaw=[pathRaw"""+cutoffRaw+r"""]\n",
      "rawDF=sqlContext.read.option(\"basepath\",pathRaw).parquet(*dateRangeRaw)"
    ],
    "metadata": {},
    "execution_count": null,
    "outputs": []
  }"""

		Notebook += r""",
	{
    "cell_type": "code",
    "source": [
      "rawDF.select(\""""+('\\", \\"').join(Campos)+r"""\").show(15,truncate=False)"
    ],
    "metadata": {},
    "execution_count": null,
    "outputs": []
  }"""


		if len(CamposLlavesMaster)>0:

			Notebook += r""",
		{
		"cell_type": "markdown",
		"metadata": {},
		"source": [
		"## Campos Llave: """+(" / ").join(CamposLlavesMaster["Physical Name field"].tolist())+r""""
		]
		}"""

			for i,Campos in CamposLlavesMaster.iterrows():
				marca = '"#'
				if(Campos["Logical Format"].replace(" ","") in ["TIMESTAMP","DATE"]):
					marca = '"'
				Notebook += r""",
		{
		"cell_type": "markdown",
		"metadata": {},
		"source": [
		"### """+Campos["Physical Name field"]+r""""
		]
		},
		{
		"cell_type": "markdown",
		"metadata": {},
		"source": [
		"#### Valores Nulos:"
		]
		},
	  {
	    "cell_type": "code",
	    "source": [
	      "masterDF.filter(col(\""""+Campos["Physical Name field"]+r"""\").isNull()).groupBy(\""""+Campos["Physical Name field"]+r"""\").count().limit(20).show(20)"
	    ],
	    "metadata": {},
	    "execution_count": null,
	    "outputs": []
	  },
		{
		"cell_type": "markdown",
		"metadata": {},
		"source": [
		"### Valores Vacio:"
		]
		},
	  {
	    "cell_type": "code",
	    "source": [
	      "masterDF.filter(trim(col(\""""+Campos["Physical Name field"]+r"""\")) == \"\").count()"
	    ],
	    "metadata": {},
	    "execution_count": null,
	    "outputs": []
	  },
		{
		"cell_type": "markdown",
		"metadata": {},
		"source": [
		"#### Longitud el valor:"
		]
		},
	  {
	    "cell_type": "code",
	    "source": [
	      "masterDF.groupBy(length(col(\""""+Campos["Physical Name field"]+r"""\"))).count().limit(20).show(20)"
	    ],
	    "metadata": {},
	    "execution_count": null,
	    "outputs": []
	  },
		{
		"cell_type": "markdown",
		"metadata": {},
		"source": [
		"#### Muestra de valor:"
		]
		},
	  {
	    "cell_type": "code",
	    "source": [
	      "masterDF.groupBy(col(\""""+Campos["Physical Name field"]+r"""\")).count().limit(20).show(20,truncate=False)"
	    ],
	    "metadata": {},
	    "execution_count": null,
	    "outputs": []
	  },
		{
		"cell_type": "markdown",
		"metadata": {},
		"source": [
		"#### Fecha mínima:"
		]
		},
	  {
	    "cell_type": "code",
	    "source": [
	      """+marca+r"""masterDF.agg(min(col(\""""+Campos["Physical Name field"]+r"""\"))).show(truncate=False)"
	    ],
	    "metadata": {},
	    "execution_count": null,
	    "outputs": []
	  },
		{
		"cell_type": "markdown",
		"metadata": {},
		"source": [
		"#### Fecha máxima:"
		]
		},
	  {
	    "cell_type": "code",
	    "source": [
	      """+marca+r"""masterDF.agg(max(col(\""""+Campos["Physical Name field"]+r"""\"))).show(truncate=False)"
	    ],
	    "metadata": {},
	    "execution_count": null,
	    "outputs": []
	  }"""
		

		else:
			Notebook += r""",
		{
		"cell_type": "markdown",
		"metadata": {},
		"source": [
		"## Campos Llave: No Posee"
		]
		}"""
		Notebook += r"""
  ],
"metadata": {
	"kernelspec": {
		"display_name": "spark-python",
		"language": "python",
		"name": "spark-python-spark-python"
  },
	"language_info": {
		"codemirror_mode": {
		"name": "ipython",
		"version": 3
	},
	"file_extension": ".py",
	"mimetype": "text/x-python",
	"name": "python",
	"nbconvert_exporter": "python",
	"pygments_lexer": "ipython3",
	"version": "3.6.13"
	}
},
"nbformat": 4,
"nbformat_minor": 4
}"""

		f = open (f'./Output/{row["TABLA MASTER"]} - Reliability.ipynb','w', encoding='utf-8')
		f.write(Notebook)
		f.close()

