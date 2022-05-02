import urllib.request as request
import json
import cx_Oracle

#Ativando o instant client para execução
cx_Oracle.init_oracle_client(lib_dir=r"C:\oracle\instantclient_19_11")

#Estabelecendo a conexão com o banco oracle
oracle_conn = cx_Oracle.connect(user="DEP_SGP", password="DEP_SGP01",
                               dsn="sdbdf1018/SUBMISSAO")

#Criando cursor oracle
o_cursor = oracle_conn.cursor()

#Coletando os dados do SIORG da ANAC
nr_orgao = 86144
with request.urlopen('http://estruturaorganizacional.dados.gov.br/doc/estrutura-organizacional/completa.json?codigoPoder={}&codigoEsfera={}&codigoUnidade={}&retornarOrgaoEntidadeVinculados={}'.format(1,1,nr_orgao,'SIM')) as response:
    if response.getcode() == 200:
        source = response.read()
        data = json.loads(source)
        lista_dicionarios = []
        lista_SIORG = []
        lista_SIORG_CONTATO = []
        lista_SIORG_ENDERECO = []
        for i in range(0, len(data['unidades'])):
            codigoUnidade = (data['unidades'][i]['codigoUnidade'])[70:]
            codigoUnidadePai = (data['unidades'][i]['codigoUnidadePai'])[70:]
            codigoTipoUnidade = (data['unidades'][i]['codigoTipoUnidade'])[60:]
            nome = (data['unidades'][i]['nome'])
            sigla = (data['unidades'][i]['sigla'])
            contato = (data['unidades'][i]['contato'])
            areaAtuacao = (data['unidades'][i]['areaAtuacao'])
            endereco = (data['unidades'][i]['endereco'])
            
            #Criando uma lista de dicionários com os dados da ANAC
            dicioario = {
                'codigoUnidade': codigoUnidade,
                'codigoUnidadePai': codigoUnidadePai,
                'codigoTipoUnidade': codigoTipoUnidade,
                'nome': nome,
                'sigla': sigla,
                'contato': contato,
                'areaAtuacao': areaAtuacao,
                'endereco': endereco
            }
            lista_dicionarios.append(dicioario)

            #Criando a lista de dados Unidades SIORG da ANAC
            linha_lista_SIORG = [
                codigoUnidade,
                codigoUnidadePai,
                codigoTipoUnidade,
                nome,
                sigla,
                areaAtuacao
                ]
            lista_SIORG.append(linha_lista_SIORG)

            #Criando a lista de contato Unidades SIORG da ANAC
            for c in range(0,len(data['unidades'][i]['contato'])):
    
                contatoTelefones = ''
                contatoEmail = ''
                contatoSite = ''

                #Evitando erros com os conjuntos
                if data['unidades'][i]['contato'][c]['telefone'] == []:
                    contatoTelefone = 'None'

                    if data['unidades'][i]['contato'][c]['email'] == []:
                        contatoEmail = 'None'

                        if data['unidades'][i]['contato'][c]['telefone'] == []:
                            contatoSite = 'None'
                                        
                        elif len(data['unidades'][i]['contato'][c]['telefone']) > 0:
                            contatoSite = data['unidades'][i]['contato'][c]['telefone'][0]
                    
                    elif len(data['unidades'][i]['contato'][c]['email']) > 0:
                        contatoEmail = data['unidades'][i]['contato'][c]['email'][0]
                
                        if data['unidades'][i]['contato'][c]['telefone'] == []:
                            contatoSite = 'None'
                            
                        
                        elif len(data['unidades'][i]['contato'][c]['telefone']) > 0:
                            contatoSite = data['unidades'][i]['contato'][c]['telefone'][0]

                elif len(data['unidades'][i]['contato'][c]['telefone']) > 0:
                    contatoTelefone = data['unidades'][i]['contato'][c]['telefone'][0]

                    if data['unidades'][i]['contato'][c]['email'] == []:
                        contatoEmail = 'None'
                        
                        if data['unidades'][i]['contato'][c]['telefone'] == []:
                            contatoSite = 'None'
                            
                        
                        elif len(data['unidades'][i]['contato'][c]['telefone']) > 0:
                            contatoSite = data['unidades'][i]['contato'][c]['telefone'][0]
                    
                    elif len(data['unidades'][i]['contato'][c]['email']) > 0:
                        contatoEmail = data['unidades'][i]['contato'][c]['email'][0]

                        if data['unidades'][i]['contato'][c]['telefone'] == []:
                            contatoSite = 'None'
                            
                        
                        elif len(data['unidades'][i]['contato'][c]['telefone']) > 0:
                            contatoSite = data['unidades'][i]['contato'][c]['telefone'][0]
    
                linha_lista_SIORG_CONTATO = [codigoUnidade ,contatoTelefone, contatoEmail, contatoSite]
                lista_SIORG_CONTATO.append(linha_lista_SIORG_CONTATO)

            #Criando a lista de endereco Unidades SIORG da ANAC
            for e in range(0,len(data['unidades'][i]['endereco'])):
                linha_lista_SIORG_ENDERECO = [
                    codigoUnidade,
                    data['unidades'][i]['endereco'][e]['logradouro'],
                    data['unidades'][i]['endereco'][e]['numero'],
                    data['unidades'][i]['endereco'][e]['complemento'],
                    data['unidades'][i]['endereco'][e]['bairro'],
                    data['unidades'][i]['endereco'][e]['cep'],
                    data['unidades'][i]['endereco'][e]['uf'],
                    data['unidades'][i]['endereco'][e]['municipio'],
                    data['unidades'][i]['endereco'][e]['pais'],
                    data['unidades'][i]['endereco'][e]['tipoEndereco'],
                    data['unidades'][i]['endereco'][e]['horarioDeFuncionamento']
                ]
                lista_SIORG_ENDERECO.append(linha_lista_SIORG_ENDERECO)

        # Populando a tabela UORG_SIORG com os dados da ANAC
        o_cursor.executemany('''insert into UORG_SIORG(
            NR_UORG,
            NR_UORG_PAI,
            NR_ORGAO,
            DS_TIPO,
            NM_UORG,
            SG_UORG,
            DS_AREA_ATUACAO
        )
        values
        (:1,:2,'86144',:3,:4,:5,:6)
        ''', lista_SIORG)

        # Populando a tabela UORG_SIORG_CONTATO com os dados da ANAC
        o_cursor.executemany('''insert into UORG_SIORG_CONTATO(
            ID_UORG,
            NR_TELEFONE,
            NM_EMAIL,
            NM_SITE
        )
        values
        (:1,:2,:3,:4)
        ''', lista_SIORG_CONTATO)

        # Populando a tabela UORG_SIORG_ENDERECO com os dados da ANAC
        o_cursor.executemany('''insert into UORG_SIORG_ENDERECO(
            ID_UORG,
            DS_LOGRADOURO,
            NR_ENDERECO,
            DS_COMPLEMENTO,
            DS_BAIRRO,
            NR_CEP,
            SG_UF,
            NR_MUNICIPIO,
            NR_PAIS,
            DS_TIPO_ENDERECO,
            DS_HORARIO_FUNICIONAMENTO
        )
        values
        (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11)
        ''', lista_SIORG_ENDERECO)

        # Commitando/confirmando alterações
        oracle_conn.commit()
        print('Dados comitados')
    else:
        print('An error occurred while attempting to retrieve data from the API.')