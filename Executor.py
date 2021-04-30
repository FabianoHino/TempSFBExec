# -*- coding: utf-8 -*-
#Esri start of added imports
import sys, os, arcpy
# Esri end of added imports

# Esri start of added variables
g_ESRI_variable_1 = os.path.join(arcpy.env.packageWorkspace,u'Z:\\data\\arcgis\\server\\usr\\directories\\arcgissystem\\arcgisinput\\CAR\\Executor.GPServer\\extracted\\v101\\insumos.sde\\insumos.sde.insumos')
g_ESRI_variable_2 = os.path.join(arcpy.env.packageWorkspace,u'Z:\\data\\arcgis\\server\\usr\\directories\\arcgissystem\\arcgisinput\\CAR\\Executor.GPServer\\extracted\\v101\\resultados.sde\\resultados.sde.resultados')
g_ESRI_variable_3 = os.path.join(arcpy.env.packageWorkspace,u'Z:\\data\\arcgis\\server\\usr\\directories\\arcgissystem\\arcgisinput\\CAR\\Executor.GPServer\\extracted\\v101\\resultados.sde\\resultados.sde.template')
g_ESRI_variable_4 = os.path.join(arcpy.env.packageWorkspace,u'logs')
g_ESRI_variable_5 = u'in_memory/rios_imovel_dissolve'
g_ESRI_variable_6 = u'CLASSE'
g_ESRI_variable_7 = u'in_memory/apps_imovel_dissolve'
# Esri end of added variables

# #############

import arcpy
import urllib2
import datetime
import json
import codecs
import traceback
import os
from functools import partial
import restart_geoprocessing

### Configurações da Ferramenta
version = '2.2.2'
insumos_db = g_ESRI_variable_1 #'/data/arcgis/server/usr/directories/arcgissystem/arcgisinput/CAR/Executor.GPServer/extracted/v101/insumos.sde'
insumos_prefix = ''
result_db = g_ESRI_variable_2 #'/data/arcgis/server/usr/directories/arcgissystem/arcgisinput/CAR/Executor.GPServer/extracted/v101/resultados.sde.resultados'
result_prefix = 'resultados.sde.'
template_fc = g_ESRI_variable_3 #'/data/arcgis/server/usr/directories/arcgissystem/arcgisinput/CAR/Executor.GPServer/extracted/v101/resultados.sde.template'
log_dir = '/storage/car/executor/logs' #g_ESRI_variable_4
input_dir = None# r'D:/CAR/inputs'
url_retorno = 'http://cearcpd01.florestal.gov.br/car/imovel/update'
retorno_retries = 5
salvar_poligonos_input = False
arcpy.env.overwriteOutput = True

### Variáveis Globais
global log_file
global id_imovel

classes_rios = {
    1: 'sobreposicaoHidrografiaClassificadaRioAte10',
    2: 'sobreposicaoHidrografiaClassificadaRio10a50',
    3: 'sobreposicaoHidrografiaClassificadaRio50a200',
    4: 'sobreposicaoHidrografiaClassificadaRio200a600',
    5: 'sobreposicaoHidrografiaClassificadaRioAcima600',
    6: 'sobreposicaoHidrografiaClassificadaLagoNatural',
    7: 'sobreposicaoHidrografiaClassificadaReservatorioArtificial',
}

classes_apps = {
    1: 'sobreposicaoAPPClassificadaRioAte10',
    2: 'sobreposicaoAPPClassificadaRio10a50',
    3: 'sobreposicaoAPPClassificadaRio50a200',
    4: 'sobreposicaoAPPClassificadaRio200a600',
    5: 'sobreposicaoAPPClassificadaRioAcima600',
    6: 'sobreposicaoAPPClassificadaLagoNatural',
    7: 'sobreposicaoAPPClassificadaReservatorioArtificial',
}

tamanho_buffer_rios = {
    1: 30,
    2: 50,
    3: 100,
    4: 200,
    5: 500,
    6: 50,
    7: 50,
}
temas_declarados_app = [
    'APP_NASCENTE_OLHO_DAGUA',
    'APP_LAGO_NATURAL',
    'APP_RESERVATORIO_ARTIFICIAL_DECORRENTE_BARRAMENTO',
    'APP_VEREDA',
    'APP_AREA_TOPO_MORRO',
    'APP_MANGUEZAL',
    'APP_AREA_ALTITUDE_SUPERIOR_1800',
    'APP_BORDA_CHAPADA',
    'APP_RESTINGA',
    'APP_AREA_DECLIVIDADE_MAIOR_45',
    'APP_BANHADO',
    'APP_RESERVATORIO_GERACAO_ENERGIA_ATE_24_08_2001',
    'APP_ESCADINHA_NASCENTE_OLHO_DAGUA'
]

temas_declarados_app_especial = [
    'APP_NASCENTE_OLHO_DAGUA',
    'APP_VEREDA',
    'APP_AREA_TOPO_MORRO',
    'APP_MANGUEZAL',
    'APP_AREA_ALTITUDE_SUPERIOR_1800',
    'APP_BORDA_CHAPADA',
    'APP_RESTINGA',
    'APP_AREA_DECLIVIDADE_MAIOR_45',
    'APP_BANHADO'
]

### Tabelas e FCs
VEGETACAO_2008 = insumos_prefix + 'VEGETACAO_2008'
VEGETACAO_ATUAL = insumos_prefix + 'VEGETACAO_2019'
AREA_ANTROPIZADA = insumos_prefix + 'AREA_ANTROPIZADA'
AREA_CONSOLIDADA = insumos_prefix + 'AREA_CONSOLIDADA'
HIDROGRAFIA = insumos_prefix + 'HIDROGRAFIA'
AMAZONIA_LEGAL_CAMPO = insumos_prefix + 'AMAZONIA_LEGAL_CAMPO' 
AMAZONIA_LEGAL_CERRADO = insumos_prefix + 'AMAZONIA_LEGAL_CERRADO'
AMAZONIA_LEGAL_FLORESTA = insumos_prefix + 'AMAZONIA_LEGAL_FLORESTA'
ZEE_OUTRAS_ZONAS = insumos_prefix + 'ZEE_OUTRAS_ZONAS'
ZEE_ZONA_CONSOLIDACAO_EXPANSAO = insumos_prefix + 'ZEE_ZONA_CONSOLIDACAO_EXPANSAO'
APP = insumos_prefix + 'APP' #BASE_APP_HIDROGRAFICA

### Referencia Espacial
wgs84 = arcpy.SpatialReference(4326)


### Métodos úteis
def log(id, msg):
    line = '[{2}]IMOVEL {0}: {1}' .format(id, msg, datetime.datetime.now())
    arcpy.AddMessage(line)
    if log_file:
        log_file.write(line)
        log_file.write('\n')


def create_fc(name, db = result_db):
    arcpy.CreateFeatureclass_management(db, name, "POLYGON",
                                        template_fc, spatial_reference=wgs84)


def upsert(table, key_fields, keys, fields, values):
    where_clause = ' AND '.join(["{0}={1}".format(key_fields[i], "'{}'".format(keys[i]) if isinstance(keys[i], basestring) else keys[i]) for i in range(len(keys))])
    with arcpy.da.UpdateCursor(table, fields, where_clause) as updater:
        try:
            row = updater.next()
            updater.updateRow(values)
        except StopIteration:
            with arcpy.da.InsertCursor(table, key_fields + fields) as inserter:
                inserter.insertRow(keys + values)


def report_status(id, status):
    data = json.dumps(status)
    clen = len(data)
    for i in range(retorno_retries):
        try:
            req = urllib2.Request(url_retorno,  data, {'Content-Type': 'application/json', 'Content-Length': clen})
            urllib2.urlopen(req)
            print 'Result sent!'
            break
        except Exception as exc:
            print str(exc)
            pass


def report_success(id, result):
    result.update({"idImovel": id, "status": "FINALIZADO"})
    report_status(id, result)


def report_failure(id, message):
    result = {"idImovel": id_imovel, "status": "ERRO_NO_PROCESSAMENTO", "mensagem": message}
    report_status(id, result)


def salva_tema(json_temas, nome_tema):
    #log(id_imovel, 'Carregando tema ' + nome_tema)
    temas = filter(lambda item: item["tipo"] == nome_tema, json_temas)
    if len(temas) == 0:
        raise KeyError("Tema '{0}' nao encontrado no json informado".format(nome_tema))
    shapes = []
    for tema in temas:
        if 'geoJson' not in tema:
            raise ValueError("Tema '{0}' nao possui o atributo 'geoJson'".format(nome_tema))
        tema = tema['geoJson']
        if 'coordinates' not in tema:
            raise ValueError("geojson mal-formado no tema '{0}'".format(nome_tema))

        if(tema['type'] == "MultiPolygon"):
            shapes.append(geojson_coords_to_polygon(tema['coordinates']))
        elif(tema['type'] == "Polygon"):
            shapes.append(geojson_coords_to_polygon(tema['coordinates'], True))

    return dissolve_poligonos(shapes)


def geojson_coords_to_polygon(coords, isPolygon = False):
    if not coords or len(coords) == 0:
        return None
    if(isPolygon):
        rings = []
        for linear_ring in coords:
            if linear_ring[0] == linear_ring[-1]:
                return arcpy.Polygon(arcpy.Array([arcpy.Point(*coords) for coords in linear_ring[:-1]]), wgs84)
            else:
                raise Exception('Invalid Polygon')
    else:
        parts = []
        for part in coords:
            rings = []
            for linear_ring in part:
                if linear_ring[0] == linear_ring[-1]:
                    rings.append(arcpy.Polygon(arcpy.Array([arcpy.Point(*coords) for coords in linear_ring[:-1]]), wgs84))
                else:
                    raise Exception('Invalid Polygon')
            polygon = rings.pop(0)
            for ring in rings:
                polygon = polygon.difference(ring)
            parts.append(polygon)
        union = parts.pop(0)
        for part in parts:
            union = union.union(part)
        return union

def salva_tema_se_existe(json_temas, nome_tema):
    try:
        return salva_tema(json_temas, nome_tema)
    except KeyError:
        log(id_imovel, 'Tema {} nao encontrado no JSON de entrada'.format(nome_tema))


def timefy(func):
    start = datetime.datetime.now()
    to_return = func()
    log(id_imovel, 'Execution time: {0}'.format(datetime.datetime.now() - start))
    return to_return


### Métodos auxiliares
def get_hectares(shape):
    return shape.getArea(units='HECTARES') if shape else 0


def sobreposicao_poligono_camada(poligono, camada, dissolve=True):
    log(id_imovel, 'Classificando tema {0}'.format(camada))
    clipped = arcpy.Clip_analysis(camada, poligono, arcpy.Geometry())
    if not clipped:
        return None
    if not dissolve:
        return clipped
    return dissolve_poligonos(clipped)


def sobreposicao_camadas(camada1, camada2, dissolve=True, keep_atributes=False):
    log(id_imovel, 'Classificando tema {0}'.format(camada2))
    if (not arcpy.Exists(camada2)):
        log(id_imovel, 'Tema {} nao encontrado na base de insumos'.format(camada2))
        return None
    if keep_atributes:
        join_attributes = 'NO_FID'
        output = arcpy.CreateUniqueName('INTERSECT', '%scratchGDB%')
    else:
        join_attributes = 'ONLY_FID'
        output = arcpy.Geometry()
    intersect = arcpy.Intersect_analysis([camada1, camada2], output, output_type='INPUT', join_attributes=join_attributes)
    if not intersect:
        return None
    if not dissolve:
        return intersect
    return dissolve_poligonos(intersect)


def sobreposicao_poligonos(pol1, pol2):
    if not pol1 or not pol2 or pol1.disjoint(pol2):
        return None
    return pol1.intersect(pol2, 4)


def uniao_poligonos(pol1, pol2):
    if not pol1:
        return pol2
    if not pol2:
        return pol1
    return pol1.union(pol2)


def diferenca_poligonos(pol1, pol2):
    if not pol1 or not pol2:
        return pol1
    return pol1.difference(pol2)


def buffer_poligno(poligono, distance):
    if not poligono:
        return None
    if distance == 0:
        return poligono
    buf = arcpy.Buffer_analysis(poligono, arcpy.Geometry(),
                                buffer_distance_or_field="{0} Meters".format(distance), line_side="FULL", line_end_type="ROUND",
                                dissolve_option="ALL", dissolve_field="", method="PLANAR")
    return buf[0] if len(buf) > 0 else None


def dissolve_poligonos(lista_poligonos):
    if not lista_poligonos:
        return None
    lista = filter(lambda item: item, lista_poligonos)
    if len(lista) == 0:
        return None
    if len(lista) == 1:
        return lista[0]
    dissolved = arcpy.Dissolve_management(lista, arcpy.Geometry())
    return dissolved[0] if len(dissolved) > 0 else None


def calcula_area_expandida_imovel(area_imovel):
    # 1. Aplicar Buffer de 1200 metros na area do imovel
    area_expandida = buffer_poligno(area_imovel, 1200)
    layer_area_expandida = arcpy.Select_analysis(area_expandida)
    return layer_area_expandida


def classifica_rios_imovel(area_expandida):
    # 2. Realizar sobreposicao da hidrografia classificada com a área expandida
    try:
        rios_imovel = sobreposicao_camadas(area_expandida, os.path.join(insumos_db, HIDROGRAFIA),
                                           dissolve=False, keep_atributes=True)
    except Exception as e:
        raise ValueError("Erro ao acessar insumo de HIDROGRAFIA. Impossível calcular APP")

    # 3. Dividir os rios do imóvel em trechos
    # 4. Calcular a largura para cada trecho
    ## Passos 3 e 4 já preprocessados no insumo HIDROGRAFIA QUALIFICADA

    # Separa os rios do imovel por classe
    classes = {classe: None for classe in classes_rios.keys()}

    if (rios_imovel):
        rios_dissolve = arcpy.management.Dissolve(rios_imovel, 'in_memory/rios_imovel_dissolve', "CLASSE", None, "MULTI_PART", "DISSOLVE_LINES")
        arcpy.Delete_management(rios_imovel)
        with arcpy.da.SearchCursor(rios_dissolve, ['CLASSE', 'SHAPE@']) as reader:
            for row in reader:
                classes[row[0]] = row[1]
        arcpy.Delete_management(rios_dissolve)

    return classes

def classifica_app_imovel(area_expandida, classes_rios):
    if not arcpy.Exists(os.path.join(insumos_db, APP)):
        app_expandida_classes = {classe: buffer_poligno(classes_rios[classe], tamanho_buffer_rios[classe]) for classe in classes_rios}
    else:
        apps_imovel = sobreposicao_camadas(area_expandida, os.path.join(insumos_db, APP),
                                         dissolve=False, keep_atributes=True)
        # Separa as apps do imovel por classe
        apps_dissolve = arcpy.management.Dissolve(apps_imovel, 'in_memory/apps_imovel_dissolve', "CLASSE", None, "MULTI_PART", "DISSOLVE_LINES")
        arcpy.Delete_management(apps_imovel)
        app_expandida_classes = {classe: None for classe in classes_rios.keys()}
        with arcpy.da.SearchCursor(apps_dissolve, ['CLASSE', 'SHAPE@']) as reader:
            for row in reader:
                app_expandida_classes[row[0]] = row[1]
        arcpy.Delete_management(apps_dissolve)
    return app_expandida_classes


def calcula_app(area_imovel, area_expandida, hidrografia_classes, inputs, results):
    log(id_imovel, 'Calculando APP')
    # passos 1 a 4 já executados. Resultado na variável classes

    # 5. Aplicar um buffer em cada trecho de acordo com a largura identificada
    app_expandida_classes = classifica_app_imovel(area_expandida, hidrografia_classes)

    # 6. Realizar a sobreposição da "APP Expandida" com a Área do Imóvel para obter a "APP do Imóvel"
    app_imovel_classes = {classe: sobreposicao_poligonos(app_expandida_classes[classe], area_imovel) for classe in app_expandida_classes}

    return app_imovel_classes

def calcula_app_escadinha(area_imovel, classes, results, mf):
    log(id_imovel, 'Calculando APP Escadinha para {} modulos fiscais '.format(mf))
    print 'buffer escadinha'

    if not results['sobreposicaoACClassificada']:
        results['sobreposicaoAPPEscadinhaClassificada'] = None
        return

    # passos 1 a 4 já executados. Resultado na variável classes
    print 'buffer escadinha'

    # 5. Aplicar um buffer em cada trecho de acordo com o numero de mf e a largura encontrada
    if mf <= 1:
        tamanho_buffer_escadinha = {classe: 5 for classe in classes_rios.keys()}
        tamanho_buffer_escadinha[7] = 0  # reservatorio artificial
    elif mf <= 2:
        tamanho_buffer_escadinha = {classe: 8 for classe in classes_rios.keys()}
        tamanho_buffer_escadinha[7] = 0  # reservatorio artificial
    elif mf <= 4:
        tamanho_buffer_escadinha = {classe: 15 for classe in classes_rios.keys()}
        tamanho_buffer_escadinha[7] = 0  # reservatorio artificial
    else:
        tamanho_buffer_escadinha = {classe: 100 for classe in classes_rios.keys()}
        tamanho_buffer_escadinha[1] = 20 if mf <=10 else 30 # rio ate 10
        tamanho_buffer_escadinha[2] = 30  # rio 10 a 50
        tamanho_buffer_escadinha[3] = 60  # rio 50 a 200, valor médio da  largura/2. Verificar como calcular largura
        tamanho_buffer_escadinha[6] = 30  # lago natural.
        tamanho_buffer_escadinha[7] = 0  # reservatorio artificial
    # tratamento para app_escadinha de lago natural
    log(id_imovel, 'Area lago {}'.format(get_hectares(classes[6])))
    if get_hectares(classes[6]) < 1:
        tamanho_buffer_escadinha[6] = 0


    buffer_rios = []
    for classe in classes:
        if classe in tamanho_buffer_escadinha and isinstance(tamanho_buffer_escadinha[classe], (int, long)):
            print 'aplicando buffer. classe={}, buffer={}'.format(classe, tamanho_buffer_escadinha[classe])
            buffer_rios.append(buffer_poligno(classes[classe], tamanho_buffer_escadinha[classe]))
        else:
            buffer_rios.append(classes[classe])


    buffer_rios = [buffer_poligno(classes[classe], tamanho_buffer_escadinha[classe]) for classe in classes]
    app_escadinha_expandida = dissolve_poligonos(buffer_rios)

    # 6. Realizar a sobreposição da "APP de Escadinha Expandida" com a Área do Imóvel para obter a "APP de Escadinha do Imóvel"
    app_escadinha_imovel = sobreposicao_poligonos(app_escadinha_expandida, area_imovel)

    # 7. Realizar a sobreposição da "APP de Escadinha do Imóvel" com a "Área Consolidada Classificada" para obter a "APP de Escadinha do Imóvel em AC"
    app_escadinha_em_ac = sobreposicao_poligonos(app_escadinha_imovel, results['sobreposicaoACClassificada'])

    # 8. Recortar a Hidrografia Classificada da "APP de Escadinha do Imóvel em AC" para obter a "APP de Escadinha Classificada"
    results['sobreposicaoAPPEscadinhaClassificada'] = diferenca_poligonos(app_escadinha_em_ac, results['sobreposicaoHidrografiaClassificada'])

# def calcula_app_escadinha_classificada_nascente(escadinha_nascente_olho_dagua, area_consolidada, hidrografia, results):
#     log(id_imovel, 'Calculando APP Escadinha Classificada Nascente')
#     print 'buffer escadinha classificada nascente'
#     # 1. Aplicar um buffer de 15 metros no tema declarado "APP_ESCADINHA_NASCENTE_OLHO_DAGUA" para obter a 
#     # "APP de Escadinha de Nascente do Imóvel".
#     app_escadinha_nascente_imovel = buffer_poligno(escadinha_nascente_olho_dagua, 15)
#     # 2. Realizar a sobreposição da "APP de Escadinha de Nascente do Imóvel" com a "BASE_AC" para obter a 
#     # "APP de Escadinha de Nascente do Imóvel em AC"
#     app_escadinha_nascente_imovel_ac = sobreposicao_poligonos(app_escadinha_nascente_imovel, area_consolidada)
#     # 3. Recortar a Hidrografia Classificada (BASE_HIDROGRAFIA) da "APP de Escadinha de Nascente do Imóvel em AC" 
#     # para obter a "APP de Escadinha de Nascente Classificada" do imóvel (Cálculo de diferença: "APP de Escadinha de 
#     # Nascente do Imóvel em AC" - "BASE_HIDROGRAFIA")
#     results['appEscadinhaClassificadaNascente'] = diferenca_poligonos(app_escadinha_nascente_imovel_ac, results['sobreposicaoHidrografiaClassificada'])  

def processa_imovel(idImovel, jsonImovelFile):
    # Sanity Check
    if idImovel == "-1":
        arcpy.AddMessage('Executor CAR v{0}'.format(version))
        arcpy.AddMessage('Database Insumos... ' + ('OK' if arcpy.Exists(insumos_db) else 'NOK'))
        arcpy.AddMessage('Insumos Hidrografia... ' + ('OK' if arcpy.Exists(os.path.join(insumos_db, HIDROGRAFIA)) else 'NOK'))
        arcpy.AddMessage('Database Resultados... ' + ('OK' if arcpy.Exists(result_db) else 'NOK'))
        arcpy.AddMessage('Template FC... ' + ('OK' if arcpy.Exists(template_fc) else 'NOK'))
        if log_dir:
            arcpy.AddMessage('Log Folder... ' + ('OK' if os.path.exists(log_dir) else 'NOK'))
        exit(0)

    global id_imovel
    global log_file
    if log_dir:
        log_file = codecs.open('{0}/{1}.log'.format(log_dir, idImovel), 'w', 'utf-8')
    else:
        log_file = None
    id_imovel = idImovel
    with codecs.open(jsonImovelFile, 'r', 'latin-1') as json_file:
        jsonImovel = json_file.read()

    try:
        log(idImovel, 'Executor CAR v{0}'.format(version))
        log(idImovel, 'INICIANDO PROCESSAMENTO')
        if input_dir:
            with codecs.open('{0}/{1}.json'.format(input_dir, idImovel), 'w', 'utf-8') as input_file:
                input_file.write(jsonImovel)
                input_file.write('\n')



        json_dict = json.loads(jsonImovel)
        if 'geo' not in json_dict:
            raise ValueError("Atributo 'geo' nao encontrado no json informado")
        if 'imovel' in json_dict and 'modulosFiscais' in json_dict['imovel']:
            modulosFiscais = json_dict['imovel']['modulosFiscais']
        else:
            modulosFiscais = 0
        json_dict = json_dict['geo']

        poligonos_input = {}
        poligonos_input['AREA_IMOVEL'] = salva_tema(json_dict, 'AREA_IMOVEL')
        area_imovel_layer = arcpy.Select_analysis(poligonos_input['AREA_IMOVEL'])

        ### Execucao dos calculos
        poligonos_calc = {}
        # Classificacao de uso do solo
        poligonos_calc['sobreposicaoVN2008Classificada'] = timefy(partial(sobreposicao_camadas, area_imovel_layer, os.path.join(insumos_db, VEGETACAO_2008)))
        poligonos_calc['sobreposicaoVNAtualClassificada'] = timefy(partial(sobreposicao_camadas, area_imovel_layer, os.path.join(insumos_db, VEGETACAO_ATUAL))) # Alterado de 2019 para Atual
        poligonos_calc['sobreposicaoAAClassificada'] = timefy(partial(sobreposicao_camadas, area_imovel_layer, os.path.join(insumos_db, AREA_ANTROPIZADA)))
        poligonos_calc['sobreposicaoACClassificada'] = timefy(partial(sobreposicao_camadas, area_imovel_layer, os.path.join(insumos_db, AREA_CONSOLIDADA)))
        poligonos_calc['sobreposicaoHidrografiaClassificada'] = timefy(partial(sobreposicao_camadas, area_imovel_layer, os.path.join(insumos_db, HIDROGRAFIA)))

        poligonos_input['VEGETACAO_NATIVA'] = salva_tema_se_existe(json_dict, 'VEGETACAO_NATIVA')
        poligonos_input['AREA_NAO_CLASSIFICADA'] = salva_tema_se_existe(json_dict, 'AREA_NAO_CLASSIFICADA')
        poligonos_input['AREA_CONSOLIDADA'] = salva_tema_se_existe(json_dict, 'AREA_CONSOLIDADA')
        poligonos_input['ARL_TOTAL'] = salva_tema_se_existe(json_dict, 'ARL_TOTAL')
        poligonos_input['AREA_POUSIO'] = salva_tema_se_existe(json_dict, 'AREA_POUSIO')

        poligonos_calc['vnDeclaradaVNClassificada'] = sobreposicao_poligonos(poligonos_input['VEGETACAO_NATIVA'], poligonos_calc['sobreposicaoVNAtualClassificada'])
        # REMOVIDO em 06/06/2019 poligonos_calc['aaDeclaradaAAClassificada']
        poligonos_calc['acDeclaradaACClassificada'] = sobreposicao_poligonos(poligonos_input['AREA_CONSOLIDADA'], poligonos_calc['sobreposicaoACClassificada'])
        poligonos_calc['rlDeclaradaVNClassificada'] = sobreposicao_poligonos(poligonos_input['ARL_TOTAL'], poligonos_calc['sobreposicaoVNAtualClassificada'])
        poligonos_calc['rlDeclaradaACClassificada'] = sobreposicao_poligonos(poligonos_input['ARL_TOTAL'], poligonos_calc['sobreposicaoACClassificada'])
        poligonos_calc['rlDeclaradaAAClassificada'] = sobreposicao_poligonos(poligonos_input['ARL_TOTAL'], poligonos_calc['sobreposicaoAAClassificada'])
        poligonos_calc['pousioDeclaradaVNClassificada'] = sobreposicao_poligonos(poligonos_input['AREA_POUSIO'], poligonos_calc['sobreposicaoVNAtualClassificada'])

        # REMOVIDO em 06/06/2019 
        poligonos_calc['sobreposicaoAmazoniaLegalCampo'] = timefy(partial(sobreposicao_camadas, area_imovel_layer, os.path.join(insumos_db, AMAZONIA_LEGAL_CAMPO)))
        poligonos_calc['sobreposicaoAmazoniaLegalCerrado'] = timefy(partial(sobreposicao_camadas, area_imovel_layer, os.path.join(insumos_db, AMAZONIA_LEGAL_CERRADO)))
        poligonos_calc['sobreposicaoAmazoniaLegalFloresta'] = timefy(partial(sobreposicao_camadas, area_imovel_layer, os.path.join(insumos_db, AMAZONIA_LEGAL_FLORESTA)))

        ## Calculos APP
        for tema in temas_declarados_app:
            if tema not in poligonos_input:
                poligonos_input[tema] = salva_tema_se_existe(json_dict, tema)
        area_expandida = calcula_area_expandida_imovel(poligonos_input['AREA_IMOVEL'])
        rios_classificados = classifica_rios_imovel(area_expandida)
        
        for classe in classes_rios.keys():
            poligonos_calc[classes_rios[classe]] = sobreposicao_poligonos(poligonos_input['AREA_IMOVEL'], rios_classificados[classe])  # salva cada classe nos resultados
        
        app_classes = timefy(partial(calcula_app, poligonos_input['AREA_IMOVEL'], area_expandida, rios_classificados, poligonos_input, poligonos_calc))
        for classe in classes_apps.keys():
            poligonos_calc[classes_apps[classe]] = diferenca_poligonos(app_classes[classe], poligonos_calc['sobreposicaoHidrografiaClassificada'])  # salva cada classe nos resultados
                
        # 7. Unir a "APP do Imóvel" aos temas declarados no imóvel
        temas_uniao = [poligonos_input[tema] for tema in temas_declarados_app]
        #BASE_APP_HIDROGRAFICA
        app_imovel_hidrografia = dissolve_poligonos(app_classes.values()) 
        #APP + Temas Declarados
        app_imovel_final = dissolve_poligonos(app_classes.values() + temas_uniao)

        # 8. Recortar a Hidrografia Classificada da "APP do Imóvel" para obter a "APP Classificada" do imóvel
        poligonos_calc['sobreposicaoAPPClassificada'] = diferenca_poligonos(app_imovel_hidrografia,
            poligonos_calc['sobreposicaoHidrografiaClassificada'])
      
           
        #A sobreposicaoAPPTotalClassificada é igual a união da sobreposicaoAPPClassificada previamente calculada e dos temas_declarados (app_imovel_final)

        poligonos_calc['sobreposicaoAPPTotalClassificada'] = uniao_poligonos(poligonos_calc['sobreposicaoAPPClassificada'],
            app_imovel_final)     
        
        for item in poligonos_input:
            print(item + ' ' + str(get_hectares(poligonos_input[item])))

        # C:\Python27\ArcGIS10.8\python.exe Executor.py
        
        #         result = {}
        # for cod_geom in poligonos_calc:
        #     result[cod_geom] = get_hectares(poligonos_calc[cod_geom])   
                
        ###

        temas_app_especial_uniao = dissolve_poligonos([poligonos_input[tema] for tema in temas_declarados_app_especial])
        poligonos_calc['appEspecialDeclaradaACClassificada'] = sobreposicao_poligonos(temas_app_especial_uniao,
                    poligonos_calc['sobreposicaoACClassificada'])
        poligonos_calc['appEspecialDeclaradaAAClassificada'] = sobreposicao_poligonos(temas_app_especial_uniao,
                    poligonos_calc['sobreposicaoAAClassificada'])


        poligonos_input['APP_TOTAL'] = salva_tema_se_existe(json_dict, 'APP_TOTAL')
        
        #calculo desativado
        # poligonos_calc['appDeclaradaAppClassificada'] = sobreposicao_poligonos(poligonos_input['APP_TOTAL'], poligonos_calc['sobreposicaoAPPClassificada'])
        
        #novo cálculo
        poligonos_calc['appDeclaradaAppClassificada'] = sobreposicao_poligonos(poligonos_input['APP_TOTAL'], poligonos_calc['sobreposicaoAPPTotalClassificada'])
        #
        
        poligonos_calc['appDeclaradaACClassificada'] = sobreposicao_poligonos(poligonos_input['APP_TOTAL'], poligonos_calc['sobreposicaoACClassificada'])
        poligonos_calc['appDeclaradaAAClassificada'] = sobreposicao_poligonos(poligonos_input['APP_TOTAL'], poligonos_calc['sobreposicaoAAClassificada'])
        poligonos_calc['appClassificadaVNClassificada'] = sobreposicao_poligonos(poligonos_calc['sobreposicaoAPPClassificada'], poligonos_calc['sobreposicaoVNAtualClassificada'])
        poligonos_calc['appClassificadaACClassificada'] = sobreposicao_poligonos(poligonos_calc['sobreposicaoAPPClassificada'], poligonos_calc['sobreposicaoACClassificada'])
        poligonos_calc['appClassificadaAAClassificada'] = sobreposicao_poligonos(poligonos_calc['sobreposicaoAPPClassificada'], poligonos_calc['sobreposicaoAAClassificada'])
        poligonos_calc['appDeclaradaVNClassificada'] = sobreposicao_poligonos(poligonos_input['APP_TOTAL'], poligonos_calc['sobreposicaoVNAtualClassificada'])
        poligonos_calc['appDeclaradaVNClassAPPClassVNClass'] = sobreposicao_poligonos(poligonos_calc['appDeclaradaVNClassificada'], poligonos_calc['appClassificadaVNClassificada'])
        
        ### novos calculos
        #app_total_classificada_ac_classificada
        poligonos_calc['appTotalClassificadaACClassificada'] = sobreposicao_poligonos(poligonos_calc['sobreposicaoAPPTotalClassificada'], poligonos_calc['sobreposicaoACClassificada'])
        #app_total_classificada_aa_classificada
        poligonos_calc['appTotalClassificadaAAClassificada'] = sobreposicao_poligonos(poligonos_calc['sobreposicaoAPPTotalClassificada'], poligonos_calc['sobreposicaoAAClassificada'])
        # app_total_classificada_vn_classificada
        poligonos_calc['appTotalClassificadaVNClassificada'] = sobreposicao_poligonos(poligonos_calc['sobreposicaoAPPTotalClassificada'], poligonos_calc['sobreposicaoVNAtualClassificada'])
        # app_hidrografica_classificada_total
        poligonos_calc['appHidrograficaClassificadaTotal'] = uniao_poligonos(poligonos_input['APP_NASCENTE_OLHO_DAGUA'], app_imovel_hidrografia)
        ################ 


        # temas declarados: "ARL_TOTAL", "AREA_USO_RESTRITO_DECLIVIDADE_25_A_45", "AREA_USO_RESTRITO_PANTANEIRA" (realizar UNIAO)
        ## Calculos AREA FORA
        poligonos_input['AREA_USO_RESTRITO_DECLIVIDADE_25_A_45'] = salva_tema_se_existe(json_dict, 'AREA_USO_RESTRITO_DECLIVIDADE_25_A_45')
        poligonos_input['AREA_USO_RESTRITO_PANTANEIRA'] = salva_tema_se_existe(json_dict, 'AREA_USO_RESTRITO_PANTANEIRA')
        poligonos_input['USO_RESTRITO'] = uniao_poligonos(poligonos_input['AREA_USO_RESTRITO_DECLIVIDADE_25_A_45'], poligonos_input['AREA_USO_RESTRITO_PANTANEIRA'])

        uniaoAppRlUr = uniao_poligonos(
            poligonos_calc['sobreposicaoAPPClassificada'],
            uniao_poligonos(poligonos_input['ARL_TOTAL'], poligonos_input['USO_RESTRITO'])
        )
        areaForaAppRlUr = diferenca_poligonos(poligonos_input['AREA_IMOVEL'], uniaoAppRlUr)
        poligonos_calc['areaRecomporForaAppRlUrClassificada'] = sobreposicao_poligonos(poligonos_calc['sobreposicaoAAClassificada'], areaForaAppRlUr)
        # REMOVIDO em 06/06/2019 poligonos_calc['areaRecomporForaAppRlUrDeclarada']

        poligonos_calc['sobreposicaoZonaConsolidacaoExpansao'] = timefy(partial(sobreposicao_camadas, area_imovel_layer, os.path.join(insumos_db, ZEE_ZONA_CONSOLIDACAO_EXPANSAO)))
        poligonos_calc['sobreposicaoOutrasZonas'] = timefy(partial(sobreposicao_camadas, area_imovel_layer, os.path.join(insumos_db, ZEE_OUTRAS_ZONAS)))
        poligonos_calc['vnClassificadaZonaConsolidacaoExpansao'] = sobreposicao_poligonos(poligonos_calc['sobreposicaoVNAtualClassificada'], poligonos_calc['sobreposicaoZonaConsolidacaoExpansao'])
        poligonos_calc['aaClassificadaZonaConsolidacaoExpansao'] = sobreposicao_poligonos(poligonos_calc['sobreposicaoAAClassificada'], poligonos_calc['sobreposicaoZonaConsolidacaoExpansao'])

        ## Cálculos tema USO_RESTRITO
        poligonos_calc['urDeclaradaVNClassificada'] = sobreposicao_poligonos(poligonos_input['USO_RESTRITO'], poligonos_calc['sobreposicaoVNAtualClassificada'])
        poligonos_calc['urDeclaradaAAClassificada'] = sobreposicao_poligonos(poligonos_input['USO_RESTRITO'], poligonos_calc['sobreposicaoAAClassificada'])
        poligonos_calc['urDeclaradaACClassificada'] = sobreposicao_poligonos(poligonos_input['USO_RESTRITO'], poligonos_calc['sobreposicaoACClassificada'])
        #
        # ## Cáculo de APP Escadinha
        timefy(partial(calcula_app_escadinha, poligonos_input['AREA_IMOVEL'], rios_classificados, poligonos_calc, modulosFiscais))
        
        # Cálculo da APP Escadinha Nascente

        buffer_escadinha_nascente = buffer_poligno(poligonos_input['APP_ESCADINHA_NASCENTE_OLHO_DAGUA'], 15)
        app_escadinha_nascente_imovel = sobreposicao_poligonos(poligonos_input['AREA_CONSOLIDADA'], buffer_escadinha_nascente)
        poligonos_calc['appEscadinhaNascenteClassificada'] = diferenca_poligonos(app_escadinha_nascente_imovel, poligonos_calc['sobreposicaoVNAtualClassificada'])

        poligonos_calc['appEscadinhaHidrograficaClassificadaTotal'] = uniao_poligonos(poligonos_calc['sobreposicaoAPPEscadinhaClassificada'], poligonos_calc['appEscadinhaNascenteClassificada'])
        
        # calculo abaixo estava errado
        #poligonos_calc['sobreposicaoAppClassificadaEscadinhaClassificada'] = sobreposicao_poligonos(poligonos_calc['appHidrograficaClassificadaTotal'], poligonos_calc['appEscadinhaHidrograficaClassificadaTotal'])
        
        poligonos_calc['sobreposicaoAPPEscadinhaClassificada'] = sobreposicao_poligonos(poligonos_calc['appHidrograficaClassificadaTotal'], poligonos_calc['appEscadinhaHidrograficaClassificadaTotal'])


        #calculos inseridos 25/11/2020
    
        poligonos_calc['appClassificadaLagoNaturalAAClassificada'] = sobreposicao_poligonos(poligonos_calc['sobreposicaoAAClassificada'], poligonos_calc['sobreposicaoAPPClassificadaLagoNatural'])
        poligonos_calc['appClassificadaLagoNaturalACClassificada'] = sobreposicao_poligonos(poligonos_calc['sobreposicaoACClassificada'], poligonos_calc['sobreposicaoAPPClassificadaLagoNatural'])
        poligonos_calc['appClassificadaLagoNaturalVNClassificada'] = sobreposicao_poligonos(poligonos_calc['sobreposicaoVNAtualClassificada'], poligonos_calc['sobreposicaoAPPClassificadaLagoNatural'])

        poligonos_calc['sobreposicaoAPPClassificadaNascente'] = poligonos_input['APP_NASCENTE_OLHO_DAGUA']

        poligonos_calc['appClassificadaNascenteAAClassificada'] = sobreposicao_poligonos(poligonos_calc['sobreposicaoAAClassificada'], poligonos_calc['sobreposicaoAPPClassificadaNascente'])
        poligonos_calc['appClassificadaNascenteACClassificada'] = sobreposicao_poligonos(poligonos_calc['sobreposicaoACClassificada'], poligonos_calc['sobreposicaoAPPClassificadaNascente'])
        poligonos_calc['appClassificadaNascenteVNClassificada'] = sobreposicao_poligonos(poligonos_calc['sobreposicaoVNAtualClassificada'], poligonos_calc['sobreposicaoAPPClassificadaNascente'])

        poligonos_calc['appClassificadaReservatorioArtificialAAClassificada'] = sobreposicao_poligonos(poligonos_calc['sobreposicaoAAClassificada'], poligonos_calc['sobreposicaoAPPClassificadaReservatorioArtificial'])
        poligonos_calc['appClassificadaReservatorioArtificialACClassificada'] = sobreposicao_poligonos(poligonos_calc['sobreposicaoACClassificada'], poligonos_calc['sobreposicaoAPPClassificadaReservatorioArtificial'])
        poligonos_calc['appClassificadaReservatorioArtificialVNClassificada'] = sobreposicao_poligonos(poligonos_calc['sobreposicaoVNAtualClassificada'], poligonos_calc['sobreposicaoAPPClassificadaReservatorioArtificial'])

        poligonos_calc['appClassificadaRio10a50AAClassificada'] = sobreposicao_poligonos(poligonos_calc['sobreposicaoAAClassificada'], poligonos_calc['sobreposicaoAPPClassificadaRio10a50'])
        poligonos_calc['appClassificadaRio10a50ACClassificada'] = sobreposicao_poligonos(poligonos_calc['sobreposicaoACClassificada'], poligonos_calc['sobreposicaoAPPClassificadaRio10a50'])
        poligonos_calc['appClassificadaRio10a50VNClassificada'] = sobreposicao_poligonos(poligonos_calc['sobreposicaoVNAtualClassificada'], poligonos_calc['sobreposicaoAPPClassificadaRio10a50'])

        poligonos_calc['appClassificadaRio200a600AAClassificada'] = sobreposicao_poligonos(poligonos_calc['sobreposicaoAAClassificada'], poligonos_calc['sobreposicaoAPPClassificadaRio200a600'])
        poligonos_calc['appClassificadaRio200a600ACClassificada'] = sobreposicao_poligonos(poligonos_calc['sobreposicaoACClassificada'], poligonos_calc['sobreposicaoAPPClassificadaRio200a600'])
        poligonos_calc['appClassificadaRio200a600VNClassificada'] = sobreposicao_poligonos(poligonos_calc['sobreposicaoVNAtualClassificada'], poligonos_calc['sobreposicaoAPPClassificadaRio200a600'])

        poligonos_calc['appClassificadaRio50a200AAClassificada'] = sobreposicao_poligonos(poligonos_calc['sobreposicaoAAClassificada'], poligonos_calc['sobreposicaoAPPClassificadaRio50a200'])
        poligonos_calc['appClassificadaRio50a200ACClassificada'] = sobreposicao_poligonos(poligonos_calc['sobreposicaoACClassificada'], poligonos_calc['sobreposicaoAPPClassificadaRio50a200'])
        poligonos_calc['appClassificadaRio50a200VNClassificada'] = sobreposicao_poligonos(poligonos_calc['sobreposicaoVNAtualClassificada'], poligonos_calc['sobreposicaoAPPClassificadaRio50a200'])

        poligonos_calc['appClassificadaRioAte10AAClassificada'] = sobreposicao_poligonos(poligonos_calc['sobreposicaoAAClassificada'], poligonos_calc['sobreposicaoAPPClassificadaRioAte10'])
        poligonos_calc['appClassificadaRioAte10ACClassificada'] = sobreposicao_poligonos(poligonos_calc['sobreposicaoACClassificada'], poligonos_calc['sobreposicaoAPPClassificadaRioAte10'])
        poligonos_calc['appClassificadaRioAte10VNClassificada'] = sobreposicao_poligonos(poligonos_calc['sobreposicaoVNAtualClassificada'], poligonos_calc['sobreposicaoAPPClassificadaRioAte10'])

        poligonos_calc['appRecomporPra'] = uniao_poligonos(poligonos_calc['appTotalClassificadaAAClassificada'], poligonos_calc['sobreposicaoAPPEscadinhaClassificada'])
        poligonos_calc['appRecomporSemPra'] = uniao_poligonos(poligonos_calc['appTotalClassificadaAAClassificada'], poligonos_calc['appTotalClassificadaACClassificada'])

        poligonos_calc['sobreposicaoAPPEscadinhaClassificadaNascente'] = sobreposicao_poligonos(poligonos_calc['sobreposicaoAPPClassificadaNascente'], poligonos_calc['appEscadinhaNascenteClassificada'])
        poligonos_calc['sobreposicaoAPPEscadinhaClassificadaReservatorioArtificial'] = sobreposicao_poligonos(poligonos_calc['sobreposicaoAPPClassificadaReservatorioArtificial'], poligonos_calc['appEscadinhaHidrograficaClassificadaTotal'])
        poligonos_calc['sobreposicaoAPPEscadinhaClassificadaRio10a50'] = sobreposicao_poligonos(poligonos_calc['sobreposicaoAPPClassificadaRio10a50'], poligonos_calc['appEscadinhaHidrograficaClassificadaTotal'])
        poligonos_calc['sobreposicaoAPPEscadinhaClassificadaRio200a600'] = sobreposicao_poligonos(poligonos_calc['sobreposicaoAPPClassificadaRio200a600'], poligonos_calc['appEscadinhaHidrograficaClassificadaTotal'])
        poligonos_calc['sobreposicaoAPPEscadinhaClassificadaRio50a200'] = sobreposicao_poligonos(poligonos_calc['sobreposicaoAPPClassificadaRio50a200'], poligonos_calc['appEscadinhaHidrograficaClassificadaTotal'])
        poligonos_calc['sobreposicaoAPPEscadinhaClassificadaRioAte10'] = sobreposicao_poligonos(poligonos_calc['sobreposicaoAPPClassificadaRioAte10'], poligonos_calc['appEscadinhaHidrograficaClassificadaTotal'])

        poligonos_calc['sobreposicaoHidrografiaClassificadaLagoNatural'] = sobreposicao_poligonos(poligonos_input['AREA_IMOVEL'], poligonos_calc['sobreposicaoAPPClassificadaLagoNatural'])
        poligonos_calc['sobreposicaoHidrografiaClassificadaNascente'] = sobreposicao_poligonos(poligonos_input['AREA_IMOVEL'], poligonos_calc['sobreposicaoAPPClassificadaNascente'])

        poligonos_calc['rlRecomporClassificada'] = uniao_poligonos(poligonos_calc['rlDeclaradaACClassificada'], poligonos_calc['rlDeclaradaAAClassificada'])
        poligonos_calc['aaDeclaradaAAClassificada'] = sobreposicao_poligonos(poligonos_input['AREA_NAO_CLASSIFICADA'], os.path.join(insumos_db, AREA_ANTROPIZADA))
        poligonos_calc['urClassificadaAAClassificada'] = sobreposicao_poligonos(poligonos_calc['sobreposicaoACClassificada'], poligonos_input['USO_RESTRITO'])

        arcpy.Delete_management(area_imovel_layer)

        ### Gera resultados
        result = {}
        for cod_geom in poligonos_calc:
            result[cod_geom] = get_hectares(poligonos_calc[cod_geom])
        
        #report_success(id_imovel, result)
        
        try:
            if result_db:
                for cod_geom in poligonos_calc:
                    target_fc = os.path.join(result_db, (result_prefix + cod_geom))
                    if not arcpy.Exists(target_fc):
                        log(id_imovel, 'Criando fc ' + target_fc)
                        create_fc(cod_geom)
                    upsert(target_fc, ['ID_IMOVEL'], [id_imovel], ['SHAPE@'],
                           [poligonos_calc[cod_geom]])
                report_success(id_imovel, result)
                log(report_success(id_imovel, result), "UPDATE")
            if salvar_poligonos_input:
                cod_geom = 'AREA_IMOVEL'
                target_fc = os.path.join(result_db, (result_prefix + cod_geom))
                if not arcpy.Exists(target_fc):
                    log(id_imovel, 'Criando fc ' + target_fc)
                    create_fc(cod_geom)
                upsert(target_fc, ['ID_IMOVEL'], [id_imovel], ['SHAPE@'],
                       [poligonos_input[cod_geom]])
                report_success(id_imovel, result)
                log(report_success(id_imovel, result), "UPDATE")
        except Exception as e:

            log(idImovel, str(e))
            log(idImovel, str(traceback.print_exc()))
            
            if (str(e).split(":")[0][-6:]) == '000732':
                restart_geoprocessing.restart_executor("stop")
                restart_geoprocessing.restart_executor("start")
                report_failure(idImovel, str(e))
                time.sleep(5)
            else:
                restart_geoprocessing.restart_executor("stop")
                restart_geoprocessing.restart_executor("start")
                report_failure(idImovel, str(e))
                time.sleep(5)
            return False
        
        log(id_imovel, 'Done!')
        
        return True

    except Exception as e:
        log(idImovel, str(e))
        log(idImovel, str(traceback.print_exc()))
        
        report_failure(idImovel, str(e))

        return False

    finally:
        if log_file:
            log_file.close()
            
### main execution
# Parâmetros do serviço
if __name__ == "__main__":
    id_imovel = arcpy.GetParameterAsText(0)
    json_imovel = arcpy.GetParameterAsText(1)

    if not id_imovel:  # standalone execution
        ### Parâmetros de debug
        id_imovel = '5358'
        json_imovel = r'C:\\PROJETOS\\SFB\\JSON\\{}.json'.format(id_imovel)
    try:
        arcpy.Delete_management('in_memory')
        arcpy.Delete_management('%scratchGDB%')
        processa_imovel(id_imovel, json_imovel)
    except Exception as e:
        report_failure(idImovel, str(e))

