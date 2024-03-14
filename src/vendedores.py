import pandas as pd
from pymongo import MongoClient
from bson import ObjectId
import datetime
import os
from dotenv import load_dotenv
import sys
sys.path.append(os.getcwd())

load_dotenv()

def generate_by_campaign(campaign_id):

    # orgs

    final_results ={}
    # se nao tiver config monta ele

    print("Iniciando o cliente no banco")
    # instancia o cliente no mongo
    client = MongoClient(os.environ['MONGO_URL'])

    # instancia o banco seedz
    seedz_db = client["seedz"]
  
    # marca o nome do arqruvio de output
    arq_name = (
        f"vendedores-{campaign_id}-{datetime.datetime.now().strftime('%Y-%m-%d')}.xlsx"
    )
    # passa em cada uma das organizacoes

    campaign_results = []
    
    # escreve o id da campanha
    campaignId = ObjectId(campaign_id)


    # monta o filtro
    match = {'_id': campaignId}

    # conta os sellouts
    count = seedz_db['campaigns'].count_documents(filter=match)

    # monta os resultados, o  tamanho dos chunks e o passo
    chunk_size = 2000

    # passa em cada um dos passos do tamanho do chunk_size
    
    for step in range(0, count + 1, chunk_size):
        campaign_results.extend(list(seedz_db['campaigns'].aggregate([
            {"$match": match},
            {"$skip": step},
            {"$limit": chunk_size},
                                {
                            "$addFields": {
                                "canceledPartners": {
                                    "$map": {
                                        "input": {
                                            "$filter": {
                                                "input": "$companies",
                                                "as": "company",
                                                "cond": {
                                                    "$eq": [
                                                        "$$company.status",
                                                        "canceled"
                                                    ]
                                                }
                                            }
                                        },
                                        "as": "company",
                                        "in": "$$company.partner"
                                    }
                                }
                            }
                        },
                        {
                            "$unwind": "$companies"
                        },
                        {
                            "$match": {
                                "companies.status": "approved"
                            }
                        },
                        {
                            "$graphLookup": {
                                "from": "partners",
                                "startWith": "$companies.partner",
                                "connectFromField": "companies.partner",
                                "connectToField": "partner",
                                "as": "partners"
                            }
                        },
                        {
                            "$addFields": {
                                "partners": {
                                    "$concatArrays": [
                                        [
                                            "$companies.partner"
                                        ],
                                        {
                                            "$map": {
                                                "input": "$partners",
                                                "as": "partner",
                                                "in": "$$partner._id"
                                            }
                                        }
                                    ]
                                }
                            }
                        },
                        {
                            "$unwind": "$partners"
                        },
                        {
                            "$match": {
                                "$expr": {
                                    "$not": {
                                        "$in": [
                                            "$partners",
                                            "$canceledPartners"
                                        ]
                                    }
                                }
                            }
                        },
                        {
                            "$group": {
                                "_id": None,
                                "excludedUsers": {
                                    "$first": "$excludedUsers"
                                },
                                "partners": {
                                    "$addToSet": "$partners"
                                }
                            }
                        },
                        {
                            "$lookup": {
                                "from": "users",
                                "let": {
                                    "partners": "$partners",
                                    "excludedUsers": "$excludedUsers"
                                },
                                "pipeline": [
                                    {
                                        "$match": {}
                                    },
                                    {
                                        "$unwind": "$employees"
                                    },
                                    {
                                        "$match": {
                                            "$expr": {
                                                "$and": [
                                                    {
                                                        "$in": [
                                                            "$employees.partner",
                                                            "$$partners"
                                                        ]
                                                    }
                                                ]
                                            }
                                        }
                                    },
                                    {
                                        "$addFields": {
                                            "shouldExcluded": {
                                                "$reduce": {
                                                    "input": "$$excludedUsers",
                                                    "initialValue": False,
                                                    "in": {
                                                        "$cond": [
                                                            {
                                                                "$and": [
                                                                    {
                                                                        "$eq": [
                                                                            "$$this.user",
                                                                            "$_id"
                                                                        ]
                                                                    },
                                                                    {
                                                                        "$eq": [
                                                                            "$$this.company",
                                                                            "$employees.company"
                                                                        ]
                                                                    }
                                                                ]
                                                            },
                                                            True,
                                                            "$$value"
                                                        ]
                                                    }
                                                }
                                            }
                                        }
                                    },
                                    {
                                        "$match": {
                                            "shouldExcluded": False
                                        }
                                    },
                                    {
                                        "$lookup": {
                                            "from": "companies",
                                            "localField": "employees.company",
                                            "foreignField": "_id",
                                            "as": "company"
                                        }
                                    },
                                    {
                                        "$unwind": "$company"
                                    },
                                    {
                                        "$lookup": {
                                            "from": "partners",
                                            "localField": "employees.partner",
                                            "foreignField": "_id",
                                            "as": "partnerP"
                                        }
                                    },
                                    {
                                        "$unwind": "$partnerP"
                                    },
                                    {
                                        "$match": {}
                                    },
                                    {
                                        "$lookup": {
                                            "from": "departaments",
                                            "localField": "employees.role",
                                            "foreignField": "_id",
                                            "as": "employees.role"
                                        }
                                    },
                                    {
                                        "$unwind": {
                                            "path": "$employees.role",
                                            "preserveNullAndEmptyArrays": True
                                        }
                                    },
                                    {
                                        "$match": {}
                                    },
                                    {
                                        "$project": {
                                            "_id": 1,
                                            "name": 1,
                                            "ein": 1,
                                            "country": 1,
                                            "phone": 1,
                                            "email":1,
                                            "role._id": "$employees.role._id",
                                            "role.name": "$employees.role.name",
                                            "role.isResponsibleCTC": "$employees.role.isResponsibleCTC",
                                            "company._id": "$company._id",
                                            "company.name": "$company.name",
                                            "company.ein": "$company.ein",
                                            "company.partner": "$company.partner",
                                            "industry": "$employees.industry",
                                            "partner": "$employees.partner",
                                            "partnerP": "$partnerP.name",
                                            "partnerPEin": "$partnerP.ein",
                                        }
                                    }
                                ],
                                "as": "users"
                            }
                        },
                        {
                            "$unwind": "$users"
                        },
                        
                        {
                            "$project": {
                                "_id": "$users._id",
                                "name": "$users.name",
                                "phone": "$users.phone",
                                "email": "$users.email",
                                "ein": "$users.ein",
                                "company": "$users.company.name",
                                "companyEin": "$users.company.ein",
                                "funcao": "$users.role.name",
                                "parceiro": "$users.partnerP",
                                "parceiroEin": "$users.partnerPEin"
                            }
                        },
                        {
                            "$sort": {
                                "company.name": 1,
                                "name": 1
                            }
                        }
        ]
        )
        )
        )

    #adiciona o nome e id da camapnha
    campaign_results=[r|{'campaignId':str(campaignId)} for r in campaign_results]

    #retorna o dataframe
    return pd.json_normalize(campaign_results)

