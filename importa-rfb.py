import pandas as pd, time, os, glob

os.system('cls')

# Nomes das tabelas em relação aos tipo
nomes = {
	'EMPRECSV': 'empresas',
	'ESTABELE': 'estabelecimentos',
	'SOCIOCSV': 'socios',
	'CNAECSV': 'cnaes',
	'MOTICSV': 'motivos',
	'PAISCSV': 'paises',
	'MUNICCSV': 'municipios',
	'NATJUCSV': 'naturezas_juridicas',
	'QUALSCSV': 'qualificacoes_socios',
	'SIMPLES': 'simples_nacional'
}

# CNAEs excluídos do MEI
cnaes_excluidos = ['1012101','9609207','4520004','0161001','8211300','9609203','3812200','4789006','4784900','4541205','4771703','4771701','1742701','6920601','9603303','8122200','5812301','1742702','1121600','2052500','2063100','2062200','2061400','7319003','2391502','5611205','9603399','9102302','9603301']

# Diretório arquivos
dir = './arquivos-extraidos/'

# Função para importar tabelas
def importaTabela(tipo):

	# Lista de arquivos do tipo
	arquivos = list(glob.glob(os.path.join(dir, '*' +  tipo)))

	# Criação dos arquivos CSV vazios
	criaArquivosCSV(tipo)

	for arquivo in arquivos:
		print(f'Transformando {arquivo} em CSV...')

		#Início da contagem de tempo
		start = time.time()

		# Leitura do arquivo
		print(time.strftime('%H:%M:%S', time.localtime()) + ' - Lendo arquivo...')
		df = pd.read_csv(arquivo, sep=';', encoding='latin1', low_memory=False, dtype=str, header=None, index_col=False)
		
		# Filtragem do arquivo
		print(time.strftime('%H:%M:%S', time.localtime()) + ' - Filtrando...')
		df_filtrado = df[df[11].isin(cnaes_excluidos)]
		

		if len(df_filtrado) > 0:
			# Salvando arquivo
			df_filtrado.to_csv(f'./export/{nomes[tipo]}.csv', index=False, sep=';', encoding='latin1', header=False, mode='a')

			# Fim da contagem de tempo
			end = time.time()
			print(f"{time.strftime('%H:%M:%S', time.localtime())} - Arquivo salvo. Tempo de execução: {round(end - start,2)}s. Total de registros: {str(len(df_filtrado))}")
		else:
			print(f"{time.strftime('%H:%M:%S', time.localtime())} - Nenhum registro encontrado.")
			continue

def criaArquivosCSV(tipo):

	match tipo:
		case 'EMPRECSV':
			colunas = ['cnpj_basico', 'razao_social',
					'natureza_juridica',
					'qualificacao_responsavel',
					'capital_social_str',
					'porte_empresa',
					'ente_federativo_responsavel']

		case 'ESTABELE':
			colunas = ['cnpj_basico','cnpj_ordem', 'cnpj_dv','matriz_filial', 
              'nome_fantasia',
              'situacao_cadastral','data_situacao_cadastral', 
              'motivo_situacao_cadastral',
              'nome_cidade_exterior',
              'pais',
              'data_inicio_atividades',
              'cnae_fiscal',
              'cnae_fiscal_secundaria',
              'tipo_logradouro',
              'logradouro', 
              'numero',
              'complemento','bairro',
              'cep','uf','municipio',
              'ddd1', 'telefone1',
              'ddd2', 'telefone2',
              'ddd_fax', 'fax',
              'correio_eletronico',
              'situacao_especial',
              'data_situacao_especial']  

	df = pd.DataFrame(columns=colunas)
	df.to_csv('./export/' + nomes[tipo] + '.csv', index=False, sep=';', encoding='latin1')

importaTabela('ESTABELE')
