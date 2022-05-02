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

#Coletando os dados do SIORG
with request.urlopen('http://estruturaorganizacional.dados.gov.br//doc/cargo-funcao/alocados-estrutura') as response:
    if response.getcode() == 200:
        source = response.read()
        data = json.loads(source)
        ANAC_dicionario = []
        ANAC_lista = []
        x = 0
        #Criando uma condicional para a captação dos dados para selecionar mais a frente apenas os dados da ANAC 
        #ANAC - Agência Nacional de Aviação Civil
        while x < 78798: 
            codigoOrgao = data['cargosAlocadosEstrutura'][x]['codigoOrgao']
            if codigoOrgao == 86144:
                naturezaJuridica = data['cargosAlocadosEstrutura'][x]["naturezaJuridica"]
                categoriaUnidade = data['cargosAlocadosEstrutura'][x]["categoriaUnidade"]
                denominacaoCargo = data['cargosAlocadosEstrutura'][x]["denominacaoCargo"]
                siglaCargo = data['cargosAlocadosEstrutura'][x]["siglaCargo"]
                categoriaCargo = data['cargosAlocadosEstrutura'][x]["categoriaCargo"]
                nivelCargo = data['cargosAlocadosEstrutura'][x]["nivelCargo"]
                siglaUnidade = data['cargosAlocadosEstrutura'][x]["siglaUnidade"]
                areaAtuacao = data['cargosAlocadosEstrutura'][x]["areaAtuacao"]
                autoridade = data['cargosAlocadosEstrutura'][x]["autoridade"]
                codigoUnidade = data['cargosAlocadosEstrutura'][x]["codigoUnidade"]
                codigoUnidadePai = data['cargosAlocadosEstrutura'][x]["codigoUnidadePai"]
                denominacaoOrgao = data['cargosAlocadosEstrutura'][x]["denominacaoOrgao"]
                denominacaoUnidade = data['cargosAlocadosEstrutura'][x]["denominacaoUnidade"]
                nomeCargo = data['cargosAlocadosEstrutura'][x]["nomeCargo"]
                nrOrdem = data['cargosAlocadosEstrutura'][x]["nrOrdem"]
                total = data['cargosAlocadosEstrutura'][x]["total"]

                #Criando uma lista de Dicionários ANAC
                dicionario = {
                    "naturezaJuridica" : naturezaJuridica,
                    "categoriaUnidade" : categoriaUnidade,
                    "denominacaoCargo" : denominacaoCargo,
                    "siglaCargo" : siglaCargo,
                    "categoriaCargo" : categoriaCargo,
                    "nivelCargo" : nivelCargo,
                    "siglaUnidade" : siglaUnidade,
                    "areaAtuacao" : areaAtuacao,
                    "autoridade" : autoridade,
                    "codigoUnidade" : codigoUnidade,
                    "codigoOrgao" : codigoOrgao,
                    "codigoUnidadePai" : codigoUnidadePai,
                    "denominacaoOrgao" : denominacaoOrgao,
                    "denominacaoUnidade" : denominacaoUnidade,
                    "nomeCargo" : nomeCargo,
                    "nrOrdem" : nrOrdem,
                    "total" : total 
                }
                ANAC_dicionario.append(dicionario)

                #Criando uma Lista de dados para cada linha da lista da ANAC
                lista = [
                    naturezaJuridica,
                    categoriaUnidade,
                    denominacaoCargo,
                    siglaCargo,
                    categoriaCargo,
                    nivelCargo,
                    siglaUnidade,
                    areaAtuacao,
                    autoridade,
                    codigoUnidade,
                    codigoOrgao,
                    codigoUnidadePai,
                    denominacaoOrgao,
                    denominacaoUnidade,
                    nomeCargo,
                    nrOrdem,
                    total
                ]
                ANAC_lista.append(lista)
                x += 1

            else:
                x += 1 
        # Populando a tabela CARGOS_COMISSIONADOS_SIORG com os dados da ANAC
        o_cursor.executemany('''insert into CARGOS_COMISSIONADOS_SIORG(
            NM_NATUREZA_JURIDICA,
            DS_CATEGORIA_UNIDADE,
            DS_CARGO,
            SG_CARGO,
            NR_CATEGORIA_CARGO,
            NR_NIVEL_CARGO,
            SG_UNIDADE,
            DS_AREA_ATUACAO,
            DS_AUTORIDADE,
            NR_UNIDADE,
            NR_ORGAO,
            NR_UNIDADE_PAI,
            NM_ORGAO,
            NM_UNIDADE,
            NM_CARGO,
            NR_ORDEM,
            NR_TOTAL
        )
        values
        (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17)
        ''', ANAC_lista)

        # Commitando/confirmando alterações
        oracle_conn.commit()
        print('Dados comitados')
    else:
        print('An error occurred while attempting to retrieve data from the API.')