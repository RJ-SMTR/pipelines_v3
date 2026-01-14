{% docs consorcio %}
Consórcio ao qual o serviço pertence
{% enddocs %}

{% docs nome_consorcio %}
Nome do consórcio
{% enddocs %}

{% docs id_veiculo %}
Código identificador do veículo [número de ordem]
{% enddocs %}

{% docs datetime_ultima_atualizacao %}
Última atualização [GMT-3]
{% enddocs %}

{% docs datetime_partida %}
Horário de início da viagem
{% enddocs %}

{% docs datetime_chegada %}
Horário de fim da viagem
{% enddocs %}

{% docs distancia_planejada %}
Distância do shape [trajeto] planejado (km)
{% enddocs %}

{% docs tipo_viagem_status %}
Classificação do tipo de viagem conforme RESOLUÇÃO SMTR Nº 3843/2025
{% enddocs %}

{% docs servico %}
Serviço realizado pelo veículo
{% enddocs %}

{% docs servico_corrigido %}
Serviço realizado pelo veículo após correção
{% enddocs %}

{% docs id_viagem %}
Código único identificador da viagem
{% enddocs %}

{% docs project_id %}
Nome do projeto da GCP
{% enddocs %}

{% docs dataset_id %}
Nome do conjunto de dados
{% enddocs %}

{% docs table_id %}
Nome da tabela
{% enddocs %}

{% docs column_name %}
Nome da coluna
{% enddocs %}

{% docs data_type %}
Tipo de dado da coluna
{% enddocs %}

{% docs metadado_descricao %}
Descrição da coluna
{% enddocs %}

{% docs timestamp_gps %}
Timestamp de emissão do sinal de GPS
{% enddocs %}

{% docs data_gps %}
Data do timestamp de emissão do sinal de GPS
{% enddocs %}

{% docs hora_gps %}
Hora do timestamp de emissão do sinal de GPS
{% enddocs %}

{% docs latitude_gps %}
Parte da coordenada geográfica [eixo y] em graus decimais [EPSG:4326 - WGS84]
{% enddocs %}

{% docs longitude_gps %}
Parte da coordenada geográfica [eixo x] em graus decimais [EPSG:4326 - WGS84]
{% enddocs %}

{% docs flag_em_movimento %}
Veículos com 'velocidade' abaixo da 'velocidade_limiar_parado', são considerados como parado [false]. Caso contrário, são considerados andando [true]
{% enddocs %}

{% docs flag_trajeto_correto_hist %}
Flag de verificação se, nos últimos 'intervalo_max_desvio_segundos', ao menos algum ponto de GPS encontra-se até o limite de 'tamanho_buffer_metros' do shape da linha [Verdadeiro/Falso]
{% enddocs %}

{% docs flag_em_operacao %}
Veículos com as flags 'flag_em_movimento' e 'flag_trajeto_correto_hist' com valor true são considerados como em operação
{% enddocs %}

{% docs tipo_parada %}
Identifica veículos parados em terminais ou garagens
{% enddocs %}

{% docs flag_linha_existe_sigmob %}
Flag de verificação se a linha informada existe no SIGMOB
{% enddocs %}

{% docs flag_trajeto_correto %}
Flag de verificação se o ponto de GPS encontra-se até o limite de 'tamanho_buffer_metros' do shape da linha [true]. Se não estiver, retorna false
{% enddocs %}

{% docs status_veiculo_gps %}
"Em Operação": Quando 'flag_em_movimento' é true e 'flag_trajeto_correto_hist' é true

"Operando fora do trajeto": Quando 'flag_em_movimento' é true e 'flag_trajeto_correto_hist' é false

"Parado": Quando 'flag_em_movimento' é false:

 - Se 'tipo_parada' não é nulo, o veículo é considerado "Parado" seguido pelo tipo de parada [ex.: "Parado terminal"]

 - Se 'tipo_parada' é nulo:

    - Se 'flag_trajeto_correto_hist' é true, o status é "Parado trajeto correto"

    - Se 'flag_trajeto_correto_hist' é false, o status é "Parado fora trajeto"
{% enddocs %}

{% docs velocidade_instantanea %}
Velocidade instantânea do veículo, conforme informado pelo GPS (km/h)
{% enddocs %}

{% docs velocidade_estimada_10_min %}
Velocidade média nos últimos 10 minutos de operação (km/h)
{% enddocs %}

{% docs distancia_gps %}
Distância da última posição do GPS em relação à posição atual (m)
{% enddocs %}

{% docs versao %}
Código de controle de versão [SHA do GitHub]
{% enddocs %}

{% docs id_validador %}
Número de série do validador
{% enddocs %}

{% docs quantidade_total_transacao %}
Quantidade total de transações realizadas
{% enddocs %}

{% docs tipo_gratuidade %}
Tipo da gratuidade [Estudante, PCD, Sênior]
{% enddocs %}

{% docs tipo_pagamento %}
Tipo de pagamento utilizado
{% enddocs %}

{% docs tipo_dia %}
Dia da semana - categorias: Dia Útil, Sábado, Domingo e Ponto Facultativo
{% enddocs %}

{% docs faixa_horaria_inicio %}
Horário inicial da faixa horária
{% enddocs %}

{% docs faixa_horaria_fim %}
Horário final da faixa horária
{% enddocs %}

{% docs partidas %}
Quantidade de partidas planejadas
{% enddocs %}

{% docs quilometragem %}
Quilometragem planejada
{% enddocs %}

{% docs timestamp_captura %}
Timestamp de captura pela SMTR
{% enddocs %}

{% docs data_versao %}
Data de referência do planejamento [versão]
{% enddocs %}

{% docs content %}
Dados brutos capturados aninhados em formato JSON
{% enddocs %}

{% docs modo %}
Tipo de transporte [Ônibus, Van, BRT]
{% enddocs %}

{% docs vista %}
Itinerário do serviço [ex: Bananal ↔ Saens Peña]
{% enddocs %}

{% docs viagens_planejadas %}
Viagens planejadas no período
{% enddocs %}

{% docs inicio_periodo %}
Início do período de operação planejado
{% enddocs %}

{% docs fim_periodo %}
Fim do período de operação planejado
{% enddocs %}

{% docs feed_start_date %}
Data inicial do feed [versão] [partição]
{% enddocs %}

{% docs feed_version %}
String que indica a versão atual do conjunto de dados GTFS
{% enddocs %}

{% docs feed_end_date %}
Data final do feed [versão]
{% enddocs %}

{% docs tipo_servico %}
Tipo de serviço da linha, conforme RDO [Relatório Diário de Operação]
{% enddocs %}

{% docs ordem_servico %}
Ordem de serviço da linha, conforme RDO [Relatório Diário de Operação]
{% enddocs %}

{% docs codigo_veiculo %}
Código do Tipo de Veículo do Serviço
{% enddocs %}

{% docs tarifa_codigo %}
Tarifa determinada
{% enddocs %}

{% docs tarifa_valor %}
Tarifa Praticada
{% enddocs %}

{% docs frota_determinada %}
Frota Determinada
{% enddocs %}

{% docs frota_licenciada %}
Frota Licenciada para a linha
{% enddocs %}

{% docs frota_operante %}
Frota Operacional do dia
{% enddocs %}

{% docs qtd_viagens %}
Número de Viagens da Linha no dia
{% enddocs %}

{% docs qtd_km_cobertos %}
Quilometragem Coberta
{% enddocs %}

{% docs qtd_grt_idoso %}
Gratuidades Transportadas - Idosos
{% enddocs %}

{% docs qtd_grt_especial %}
Gratuidades Transportadas - Portadores de Necessidades Especiais
{% enddocs %}

{% docs qtd_grt_estud_federal %}
Gratuidades Transportadas - Estudantes da União
{% enddocs %}

{% docs qtd_grt_estud_estadual %}
Gratuidades Transportadas - Estudantes do Estado
{% enddocs %}

{% docs qtd_grt_estud_municipal %}
Gratuidades Transportadas - Estudantes do Município
{% enddocs %}

{% docs qtd_grt_rodoviario %}
Gratuidades Transportadas - Funcionários das Empresas
{% enddocs %}

{% docs qtd_buc_1_perna %}
Bilhete único Carioca [1ª Perna]
{% enddocs %}

{% docs qtd_buc_2_perna_integracao %}
Bilhete Único Carioca [2ª Perna]
{% enddocs %}

{% docs receita_buc %}
Receita de Bilhete Único Carioca
{% enddocs %}

{% docs qtd_buc_supervia_1_perna %}
Bilhete único Carioca - Supervia [1ª Perna]
{% enddocs %}

{% docs qtd_buc_supervia_2_perna_integracao %}
Bilhete Único Carioca - Supervia [2ª Perna]
{% enddocs %}

{% docs receita_buc_supervia %}
Receita de Bilhete Único Carioca - Supervia
{% enddocs %}

{% docs qtd_cartoes_perna_unica_e_demais %}
Cartões Perna Única e demais transações valoradas não contempladas nos campos deste RDO
{% enddocs %}

{% docs receita_cartoes_perna_unica_e_demais %}
Receita Cartões Perna Única e Receita das demais transações valoradas não contempladas nos campos deste RDO
{% enddocs %}

{% docs qtd_pagamentos_especie %}
Passageiros Pagantes em Espécie
{% enddocs %}

{% docs receita_especie %}
Receita Passageiros Pagantes em Espécie
{% enddocs %}

{% docs qtd_grt_passe_livre_universitario %}
Gratuidades Transportadas - Passe Livre Universitário
{% enddocs %}

{% docs class_servico %}
Classificação do Serviço: Diretão, Estação do BRT, Rodoviário, Troncal do BRT, Urbano
{% enddocs %}

{% docs qtd_passageiros_total %}
Quantidade total de passageiros
{% enddocs %}

{% docs numero_veiculo %}
Número de ordem do veículo
{% enddocs %}

{% docs trajeto %}
Nome do trajeto
{% enddocs %}

{% docs sentido %}
Sentido da linha
{% enddocs %}

{% docs indicador_viagem_dentro_limite %}
Indica se a viagem foi remunerada por estar abaixo do teto de 120%/200%
{% enddocs %}

{% docs indicador_penalidade_judicial %}
Indica que o valor encontra-se sob julgamento de ação judicial e será depositado em juízo
{% enddocs %}

{% docs tipo_os %}
Tipo de Ordem de Serviço [ex: 'Regular', 'Extraordinária - Verão']
{% enddocs %}

{% docs data_inicio_matriz %}
Data de inicio da versão da matriz de integração
{% enddocs %}

{% docs tecnologia %}
Tecnologia utilizada no veículo [BASICO, MIDI, MINI, PADRON, ARTICULADO]
{% enddocs %}

{% docs tecnologia_remunerada %}
Tecnologia remunerada conforme legislação em vigor
{% enddocs %}

{% docs pof %}
Indicador percentual de quilometragem apurada em relação à planejada do serviço por faixa horária [não inclui veículos não licenciados e não vistoriados a partir de 2024-09-01]
{% enddocs %}

{% docs valor_glosado_tecnologia %}
Valor da diferença entre a tecnologia remunerada e tecnologia apurada
{% enddocs %}

{% docs data_captura %}
Data da captura
{% enddocs %}

{% docs ano_fabricacao %}
Ano de fabricação do veículo
{% enddocs %}

{% docs carroceria %}
Descrição do modelo da carroceria
{% enddocs %}

{% docs data_ultima_vistoria %}
Data da última vistoria do veículo
{% enddocs %}

{% docs id_carroceria %}
Código do modelo da carroceria
{% enddocs %}

{% docs id_chassi %}
Código do modelo do chassi
{% enddocs %}

{% docs id_fabricante_chassi %}
Identificador do fabricante do chassi
{% enddocs %}

{% docs id_interno_carroceria %}
Código interno do modelo de carroceria
{% enddocs %}

{% docs id_planta %}
Código da planta do veículo
{% enddocs %}

{% docs indicador_ar_condicionado %}
Indicador se possui ar condicionado [Verdadeiro/Falso]
{% enddocs %}

{% docs indicador_elevador %}
Indicador se possui elevador [Verdadeiro/Falso]
{% enddocs %}

{% docs indicador_usb %}
Indicador se tem USB [Verdadeiro/Falso]
{% enddocs %}

{% docs indicador_wifi %}
Indicador se tem Wi-fi [Verdadeiro/Falso]
{% enddocs %}

{% docs nome_chassi %}
Descrição do modelo do chassi
{% enddocs %}

{% docs permissao %}
Número da permissão da operadora ao qual o veículo está vinculado no STU
{% enddocs %}

{% docs placa %}
Placa do veículo
{% enddocs %}

{% docs quantidade_lotacao_pe %}
Capacidade de passageiros em pé
{% enddocs %}

{% docs quantidade_lotacao_sentado %}
Capacidade de passageiros sentados
{% enddocs %}

{% docs tipo_combustivel %}
Tipo de combustível utilizado
{% enddocs %}

{% docs tipo_veiculo %}
Tipo de veículo
{% enddocs %}

{% docs status %}
Licenciado - Veículo licenciado no STU
Válido - Veículo com solicitação válida para ingresso no STU
{% enddocs %}

{% docs ano_ultima_vistoria_atualizado %}
Ano atualizado da última vistoria realizada pelo veículo
{% enddocs %}

{% docs data_inicio_vinculo %}
Data de início do vínculo do veículo no STU
{% enddocs %}

{% docs modo_brt %}
BRT – nesta tabela consta apenas este modo
{% enddocs %}

{% docs modo_sppo %}
SPPO – nesta tabela consta apenas este modo
{% enddocs %}

{% docs modo_onibus %}
ÔNIBUS – nesta tabela consta apenas este modo
{% enddocs %}

{% docs linha %}
Serviço de ônibus [linha] ou, se realocada, informada pela empresa operadora
{% enddocs %}

{% docs data_autuacao %}
Data da autuação
{% enddocs %}

{% docs data_transacao %}
Data da transação
{% enddocs %}

{% docs data_viagem %}
Data da viagem
{% enddocs %}

{% docs data %}
Data
{% enddocs %}

{% docs data_agente %}
Data de registro pelo agente público
{% enddocs %}

{% docs data_infracao %}
Data da infração
{% enddocs %}

{% docs data_inicio_parametros %}
Data inicial do período de vigência dos demais atributos
{% enddocs %}

{% docs data_limite_recurso %}
Data limite para recurso em primeira instância
{% enddocs %}

{% docs datetime_realocacao %}
Datetime_realocacao que o registro da realocação foi informado pela empresa operadora
{% enddocs %}

{% docs descricao_autuador %}
Descrição da unidade de autuação
{% enddocs %}

{% docs descricao_situacao_autuacao %}
Descrição da situação da autuação
{% enddocs %}

{% docs descricao_servico_jae %}
Nome longo da linha operada pelo veículo com variação de serviço [ex: 010, 011SN, ...] ou nome da estação de BRT na Jaé
{% enddocs %}

{% docs extensao_ida %}
Distância percorrida na ida
{% enddocs %}

{% docs id_auto_infracao %}
Código do auto de infração
{% enddocs %}

{% docs id_consorcio %}
Identificador do consórcio na tabela cadastro.consorcios
{% enddocs %}

{% docs id_registro %}
ID do registro [HASH SHA256]
{% enddocs %}

{% docs indicador_autuacao_limpeza %}
Indicador se o veículo foi autuado por infração relacionada à limpeza do veículo
{% enddocs %}

{% docs indicador_licenciado %}
Indicador se o veículo encontra-se licenciado
{% enddocs %}

{% docs km_apurada_autuado_ar_inoperante %}
Quilometragem apurada de viagens de veículos autuados por ar inoperante
{% enddocs %}

{% docs km_apurada_licenciado_sem_ar_n_autuado %}
Quilometragem apurada de viagens de veículos sem ar e não autuados
{% enddocs %}

{% docs km_apurada_n_licenciado %}
Quilometragem apurada de viagens de veículos não licenciados
{% enddocs %}

{% docs km_apurada_registrado_com_ar_inoperante %}
Distância apurada de viagens realizadas por veículo licenciado com ar condicionado e registrado por agente de verão [RESOLUÇÃO SMTR Nº 3.682/2024] em razão de inoperância ou mau funcionamento deste (km)
{% enddocs %}

{% docs link_foto %}
Link com a imagem interna do veículo
{% enddocs %}

{% docs longitude_transacao %}
Longitude da transação [WGS84]
{% enddocs %}

{% docs perc_conformidade_registros %}
Percentual de minutos da viagem com registro de sinal de GPS
{% enddocs %}

{% docs percentual_rateio %}
Percentual de rateio do valor total para a operadora
{% enddocs %}

{% docs quantidade_transacao_especie %}
Quantidade de transações feitas em espécie
{% enddocs %}

{% docs recurso_penalidade_multa %}
Número do processo de recurso contra aplicação de penalidade de multa em primeira instância
{% enddocs %}

{% docs shape %}
Shape em formato geográfico [LineString]
{% enddocs %}

{% docs shape_id %}
Código identificador do shape [trajeto]
{% enddocs %}

{% docs status_infracao %}
Descrição do status da infração
{% enddocs %}

{% docs tipificacao_resumida %}
Descrição da autuação
{% enddocs %}

{% docs trip_id %}
Código identificador de trip de referência no GTFS
{% enddocs %}

{% docs validacao %}
Coluna de validação do registro enviado pelo agente público [apenas true nesta tabela]
{% enddocs %}

{% docs valor_a_pagar %}
Valor efetivo de pagamento [valor_total_apurado - valor_acima_limite - valor_glosado]
{% enddocs %}

{% docs valor_acima_limite %}
Valor apurado das viagens que não foram remuneradas por estar acima do teto de 120% / 200%
{% enddocs %}

{% docs valor_pago %}
Valor pago da autuação (R$)
{% enddocs %}

{% docs valor_total_glosado %}
Valor total das viagens considerando o valor máximo por km, subtraído pelo valor efetivo por km (R$)
{% enddocs %}

{% docs cobertura_temporal %}
Cobertura temporal
{% enddocs %}

{% docs cep_proprietario %}
CEP do proprietário do veículo
{% enddocs %}

{% docs partidas_volta %}
Quantidade de partidas no sentido volta
{% enddocs %}

{% docs extensao %}
Distância do shape [trajeto] planejado (km)
{% enddocs %}

{% docs indicador_servico_divergente %}
Indica se o serviço indicado nos dados de GPS estava diferente do serviço informado na viagem
{% enddocs %}

{% docs taxa_conversao_real %}
Taxa da conversão de Dólar para Real
{% enddocs %}

{% docs data_fim %}
Data final do período de vigência dos demais atributos
{% enddocs %}

{% docs ano %}
Ano
{% enddocs %}

{% docs ano_indicador %}
Ano de apuração do indicador
{% enddocs %}

{% docs hora_transacao %}
Hora da transação
{% enddocs %}

{% docs data_ordem %}
Data da ordem de pagamento
{% enddocs %}

{% docs distancia_total_planejada %}
Distância total planejada do serviço [ida + volta] (km)
{% enddocs %}

{% docs hora_particao_gps %}
Hora de partição, conforme 'timestamp_captura' da tabela 'sppo_registros'
{% enddocs %}

{% docs hora_timestamp_captura %}
Hora do 'timestamp_captura'
{% enddocs %}

{% docs id_integracao %}
Identificador único da integração
{% enddocs %}

{% docs indicador_autuacao_ar_condicionado %}
Indicador se o veículo foi autuado por inoperância ou mau funcionamento do sistema de ar condicionado
{% enddocs %}

{% docs indicador_sensor_temperatura %}
Indicador se o sensor de temperatura do veículo não estava em funcionamento conforme especificação da SMTR
{% enddocs %}

{% docs irk %}
Índice de Remuneração por km (R$/km)
{% enddocs %}

{% docs km_apurada_autuado_seguranca %}
Quilometragem apurada de viagens de veículos autuados por segurança
{% enddocs %}

{% docs km_planejada_dia %}
Distância planejada para o serviço por dia (km)
{% enddocs %}

{% docs legislacao %}
Legislação que estabelece o valor e regras de remuneração
{% enddocs %}

{% docs n_registros_total %}
Contagem de sinais de GPS emitidos no tempo da viagem
{% enddocs %}

{% docs pontuacao %}
Quantidade de pontos da autuação
{% enddocs %}

{% docs servico_variacao %}
Nome curto da linha operada pelo veículo com variação de serviço [ex: 010, 011SN, ...]
{% enddocs %}

{% docs start_pt %}
Ponto inicial do shape em formato geográfico [Point]
{% enddocs %}

{% docs trip_short_name %}
Texto voltado ao público usado para identificar a viagem aos passageiros, por exemplo, para identificar os números dos trens para viagens de trens suburbanos
{% enddocs %}

{% docs valor_glosado %}
Valor total das viagens considerando o valor máximo por km, subtraído pelo valor efetivo por km (R$)
{% enddocs %}

{% docs valor_judicial %}
Valor de glosa depositada em juízo [Autuação por ar inoperante, Veículo licenciado sem ar, Penalidade abaixo de 60% e Notificação dos Agentes de Verão]
{% enddocs %}

{% docs valor_total_sem_glosa %}
Valor total das viagens considerando o valor máximo por km
{% enddocs %}

{% docs cadastro_cliente %}
Situação do cadastro do cliente na Jaé [cadastrado ou não cadastrado]
{% enddocs %}

{% docs produto %}
Produto utilizado para efetuar o pagamento no padrão utilizado na SMTR [Gratuidade, Carteira, VT, Dinheiro (Botoeira), Cartão Avulso ou Visa Internacional]
{% enddocs %}

{% docs produto_jae %}
Produto utilizado para efetuar o pagamento conforme os dados originais da Jaé
{% enddocs %}

{% docs tipo_usuario %}
Tipo do usuário que efetuou a transação [ex.: Estudante, Idoso, Pagante]
{% enddocs %}

{% docs meio_pagamento %}
Meio de pagamento utilizado no padrão utilizado na SMTR [Cartão, QRCode ou Dinheiro]
{% enddocs %}

{% docs meio_pagamento_jae %}
Meio de pagamento conforme os dados originais da Jaé
{% enddocs %}

{% docs chave %}
Chave
{% enddocs %}

{% docs codigo_enquadramento %}
Código da autuação
{% enddocs %}

{% docs datetime_captura %}
Datetime de captura do registro pela pipeline
{% enddocs %}

{% docs datetime_entrada %}
Datetime de entrada do veículo na linha realocada
{% enddocs %}

{% docs datetime_registro %}
Datetime de registro pelo agente público
{% enddocs %}

{% docs descricao_veiculo %}
Marca/modelo do veículo
{% enddocs %}

{% docs end_pt %}
Ponto final do shape em formato geográfico [Point]
{% enddocs %}

{% docs extensao_volta %}
Distância percorrida na volta
{% enddocs %}

{% docs id_empresa %}
Código identificador da empresa que opera o veículo
{% enddocs %}

{% docs id_infracao %}
Código da infração
{% enddocs %}

{% docs indicador_autuacao_seguranca %}
Indicador se o veículo foi autuado por infração relacionada à segurança do veículo
{% enddocs %}

{% docs km_apurada %}
Distância apurada para o serviço (km)
{% enddocs %}

{% docs km_apurada_autuado_limpezaequipamento %}
Quilometragem apurada de viagens de veículos autuados por limpeza ou equipamento
{% enddocs %}

{% docs km_planejada_faixa %}
Distância planejada para o serviço por faixa horária (km)
{% enddocs %}

{% docs mes %}
Mês de apuração do indicador
{% enddocs %}

{% docs n_registros_minuto %}
Contagem de minutos do trajeto com pelo menos 1 sinal de GPS emitido
{% enddocs %}

{% docs n_registros_shape %}
Contagem de sinais de GPS emitidos dentro do trajeto
{% enddocs %}

{% docs operadora %}
Nome da operadora de transporte [mascarado se for pessoa física]
{% enddocs %}

{% docs perc_conformidade_shape %}
Percentual de sinais emitidos dentro do shape [trajeto] ao longo da viagem
{% enddocs %}

{% docs route_id %}
Identificador de uma rota da tabela routes do GTFS
{% enddocs %}

{% docs sentido_bilhetagem %}
Sentido de operação do serviço [0 = ida, 1 = volta]
{% enddocs %}

{% docs servico_gps %}
Linha de ônibus [serviço] informado pelo GPS
{% enddocs %}

{% docs shape_id_planejado %}
Código identificador de shape no GTFS com ajustes
{% enddocs %}

{% docs tempo_viagem %}
Tempo aferido da viagem (minutos)
{% enddocs %}

{% docs trip_headsign %}
Texto que aparece na sinalização identificando o destino da viagem aos passageiros
{% enddocs %}

{% docs valor %}
Valor devido (R$)
{% enddocs %}

{% docs valor_apurado %}
Valor da distância apurada multiplicada pelo subsídio por quilômetro [sem glosa]. É zerado quando POF < 80%
{% enddocs %}

{% docs valor_penalidade %}
Valor penalidade [negativa] [POF <= 60%]
{% enddocs %}

{% docs viagens_dia %}
Quantidade de viagens apuradas por dia
{% enddocs %}

{% docs viagens_faixa %}
Quantidade de viagens apuradas por faixa horária
{% enddocs %}

{% docs hora_captura %}
Hora da captura
{% enddocs %}

{% docs direcao_gps %}
Direção do movimento em graus
{% enddocs %}

{% docs datetime_envio_gps %}
Data e hora em que o GPS enviou os dados de localização
{% enddocs %}

{% docs datetime_servidor_gps %}
Data e hora em que o servidor recebeu os dados de localização do GPS
{% enddocs %}

{% docs datetime_saida %}
Datetime de saída do veículo na linha realocada
{% enddocs %}

{% docs datetime_processamento %}
Datetime de processamento da realocação pela empresa operadora
{% enddocs %}

{% docs datetime_operacao %}
Datetime que o registro da realocação foi informado pela empresa operadora
{% enddocs %}

{% docs servico_realocacao %}
Linha de ônibus [serviço] realocada informada pela empresa operadora
{% enddocs %}

{% docs data_operacao %}
Data de operação
{% enddocs %}

{% docs status_veiculo %}
Classificação, observados os demais parâmetros - Categorias:

  - Não licenciado - Veículo que operou, mas não é licenciado

  - Autuado por ar inoperante - Veículo que operou, foi licenciado com ar condicionado e foi autuado por inoperância ou mau funcionamento do sistema de ar condicionado [023.II]

  - Autuado por segurança - Veículo que operou, foi licenciado, mas foi autuado por infração relacionada à segurança do veículo

  - Autuado por limpeza/equipamento - Veículo que operou, foi licenciado, mas foi autuado cumulativamente por infrações relacionadas à limpeza e equipamentos do veículo

  - Sem ar e não autuado - Veículo que operou, foi licenciado sem ar condicionado e não foi autuado

  - Com ar e não autuado - Veículo que operou, foi licenciado com ar condicionado e não foi autuado
{% enddocs %}

{% docs indicadores_veiculo %}
Indicadores para caraterização do status do veículo
{% enddocs %}

{% docs posicao_veiculo_geo %}
Coordenada geográfica [POINT] com a posição que o veículo se encontra, conforme 'longitude' e 'latitude' informadas pelo GPS [EPSG:4326 - WGS84]
{% enddocs %}

{% docs coluna %}
Nome da coluna
{% enddocs %}

{% docs data_limite_defesa_previa %}
Data limite para defesa prévia
{% enddocs %}

{% docs data_pagamento %}
Data de pagamento
{% enddocs %}

{% docs data_ultima_atualizacao %}
Data da última atualização
{% enddocs %}

{% docs datetime_processamento_viagem %}
Data e hora do processamento da viagem
{% enddocs %}

{% docs especie_veiculo %}
Espécie do veículo
{% enddocs %}

{% docs horario_inicio %}
Horário inicial de funcionamento
{% enddocs %}

{% docs indicador_autuacao_equipamento %}
Indicador se o veículo foi autuado por infração relacionada à inoperância ou mau funcionamento de equipamentos do veículo
{% enddocs %}

{% docs indicador_validador_sbd %}
Indicador se o veículo se encontra com o novo validador do Sistema de Bilhetagem Digital [BD] instalado
{% enddocs %}

{% docs infracao %}
Descrição da infração
{% enddocs %}

{% docs km_apurada_faixa %}
Distância apurada para o serviço por faixa horária (km)
{% enddocs %}

{% docs km_apurada_licenciado_com_ar_n_autuado %}
Quilometragem apurada de viagens de veículos com ar e não autuados
{% enddocs %}

{% docs km_apurada_n_vistoriado %}
Distância apurada de viagens realizadas por veículo não vistoriado tempestivamente conforme calendário de vistoria (km)
{% enddocs %}

{% docs km_subsidiada_dia %}
Distância subsidiada para o serviço por dia (km)
{% enddocs %}

{% docs ordem %}
Ordem de prioridade de aplicação dos valores de remuneração
{% enddocs %}

{% docs perc_conformidade_distancia %}
Razão da distância aferida pela distância teórica x 100
{% enddocs %}

{% docs processo_defesa_autuacao %}
Número do processo de defesa prévia
{% enddocs %}

{% docs servico_informado %}
Serviço informado pelo GPS do veículo
{% enddocs %}

{% docs shape_distance %}
Extensão do shape
{% enddocs %}

{% docs status_veiculo_infracao %}
CADASTRADA - Registrada no sistema sem guia de pagamento

EM ABERTO - Com guia de pagamento e dentro do prazo de vencimento

VENCIDA - Com guia de pagamento e fora do prazo de vencimento

EM RECURSO - Possui Processo de Recurso aguardando julgamento

PAGA - Com guia de pagamento efetivamente paga

CANCELADA - Multa foi cancelada através de um Processo de Recurso
{% enddocs %}

{% docs subsidio_km %}
Valor de subsídio de remuneração (R$/km)
{% enddocs %}

{% docs timestamp_captura_infracao %}
Timestamp de captura dos dados de infração
{% enddocs %}

{% docs timestamp_captura_licenciamento %}
Timestamp de captura dos dados de licenciamento
{% enddocs %}

{% docs timestamp_processamento %}
Timestamp de processamento da realocação pela empresa operadora
{% enddocs %}

{% docs tipo_transacao_smtr %}
Tipo de transação realizada no padrão usado pela SMTR [Tarifa Cheia, Integração e Gratuidade]
{% enddocs %}

{% docs uf_proprietario %}
Estado do proprietário do veículo
{% enddocs %}

{% docs valor_infracao %}
Valor monetário da autuação [100%] (R$)
{% enddocs %}

{% docs valor_total_apurado %}
Valor total das viagens apuradas, subtraídas as penalidades [POF =< 60%] (R$)
{% enddocs %}

{% docs inicio_vigencia_tunel %}
Data de início da vigência do túnel
{% enddocs %}

{% docs fim_vigencia_tunel %}
Data de fim da vigência do túnel
{% enddocs %}

{% docs id_execucao_dbt %}
Identificador da execução do DBT que modificou o dado pela última vez
{% enddocs %}

{% docs km_apurada_sem_transacao %}
Quilometragem apurada de viagens realizadas sem passageiro registrado
{% enddocs %}

{% docs datetime_transacao %}
Data e hora da transação [GMT-3]
{% enddocs %}

{% docs datetime_processamento_transacao %}
Data e hora de processamento da transação [GMT-3]
{% enddocs %}

{% docs id_operadora %}
Identificador da operadora na tabela cadastro.operadoras
{% enddocs %}

{% docs id_servico_jae %}
Identificador da linha no banco de dados da jaé [É possível cruzar os dados com a tabela rj-smtr.cadastro.servicos usando a coluna id_servico_jae]
{% enddocs %}

{% docs servico_jae %}
Nome curto da linha operada pelo veículo com variação de serviço [ex: 010, 011SN, ...] ou código da estação de BRT na Jaé
{% enddocs %}

{% docs id_cliente %}
Identificador único do cliente [protegido]
{% enddocs %}

{% docs id_transacao %}
Identificador único da transação
{% enddocs %}

{% docs tipo_transacao %}
Tipo de transação realizada no padrão usado pela SMTR
{% enddocs %}

{% docs latitude_transacao %}
Latitude da transação [WGS84]
{% enddocs %}

{% docs tile_id %}
Identificador do hexágono da geolocalização na tabela rj-smtr.br_rj_riodejaneiro_geo.h3_res9
{% enddocs %}

{% docs valor_transacao %}
Valor debitado na transação atual (R$)
{% enddocs %}

{% docs id_operadora_jae %}
Identificador único da operadora no sistema da Jaé
{% enddocs %}

{% docs geo_point_transacao %}
Ponto geográfico do local da transação
{% enddocs %}

{% docs quantidade_passageiros %}
Quantidade de transações que aconteceram em determinada data e hora
{% enddocs %}

{% docs documento_operadora %}
Documento do operador [CPF ou CNPJ] [protegido]
{% enddocs %}

{% docs tipo_documento_operadora %}
Tipo do documento do operador [CPF ou CNPJ]
{% enddocs %}

{% docs receita_total_esperada %}
Receita total esperada com base na quilometragem [irk * km] (R$)
{% enddocs %}

{% docs receita_tarifaria_esperada %}
Receita tarifária esperada com base na quilometragem [irk_tarifa_publica * km] (R$)
{% enddocs %}

{% docs subsidio_esperado %}
Subsídio esperado com base na quilometragem [subsidio_km * km] (R$)
{% enddocs %}

{% docs subsidio_glosado %}
Valor de subsídio glosado conforme legislação vigente [subsidio_esperado - valor_subsidio_pago] (R$)
{% enddocs %}

{% docs receita_total_aferida %}
Receita total aferida [receita_tarifaria_aferida + valor_subsidio_pago] (R$)
{% enddocs %}

{% docs receita_tarifaria_aferida %}
Receita tarifária aferida com base no RDO [Relatório Diário de Operação] (R$)
{% enddocs %}

{% docs subsidio_pago %}
Valor de subsídio efetivamente pago (R$)
{% enddocs %}

{% docs saldo %}
Saldo entre a receita esperada e a receita aferida [(receita_total_aferida - receita_total_esperada - subsidio_glosado) ou (receita_tarifaria_aferida - receita_tarifaria_esperada)] (R$)
{% enddocs %}

{% docs quinzena %}
Identificador da quinzena (1 ou 2) dentro do mês de referência
{% enddocs %}

{% docs data_inicial_quinzena %}
Data inicial da quinzena considerada
{% enddocs %}

{% docs data_final_quinzena %}
Data final da quinzena considerada
{% enddocs %}

{% docs km_subsidiada %}
Quilometragem apurada e subsidiada (km)
{% enddocs %}

{% docs datas_servico %}
Quantidade total de pares data-serviço no período
{% enddocs %}

{% docs datas_servico_pod_menor_80 %}
Quantidade de pares data-serviço com POD [Percentual de Operação Diário] menor que 80%
{% enddocs %}

{% docs datas_servico_excecao %}
Quantidade de pares data-serviço em datas de exceção
{% enddocs %}

{% docs datas_servico_atipicos %}
Quantidade de pares data-serviço atípicos
{% enddocs %}

{% docs datas_servico_ausencia_receita_tarifaria %}
Quantidade de pares data-serviço sem receita tarifária
{% enddocs %}

{% docs datas_servico_tipicos %}
Quantidade de pares data-serviço típicos
{% enddocs %}

{% docs percentual_datas_servico_pod_menor_80 %}
Percentual de pares data-serviço com POD [Percentual de Operação Diário] menor que 80%
{% enddocs %}

{% docs percentual_datas_servico_excecao %}
Percentual de pares data-serviço em datas de exceção
{% enddocs %}

{% docs percentual_datas_servico_atipicos %}
Percentual de pares data-serviço atípicos
{% enddocs %}

{% docs percentual_datas_servico_ausencia_receita %}
Percentual de pares data-serviço sem receita tarifária
{% enddocs %}

{% docs percentual_datas_servico_tipicos %}
Percentual de pares data-serviço típicos
{% enddocs %}

{% docs perc_km_planejada %}
Percentual de quilometragem apurada em relação à planejada do serviço ou POD [Percentual de Operação Diário]
{% enddocs %}

{% docs irk_tarifa_publica %}
Índice de Remuneração por Quilômetro [tarifário] (R$/km)
{% enddocs %}

{% docs tipo_recurso %}
Tipo de recurso ou justificativa que motivou a exclusão daquele par data-serviço
{% enddocs %}

{% docs tipo_inconsistencia %}
Classificação do tipo de inconsistência encontrada no par data-serviço
{% enddocs %}

{% docs km_apurada_pod %}
Distância apurada para o serviço (km) para cálculo do POD [Percentual de Operação Diário]
{% enddocs %}

{% docs servico_original_rdo %}
Serviço informado no RDO [Relatório Diário de Operação] com tratamento
{% enddocs %}

{% docs valor_subsidio_pago %}
Valor total pago de subsídio
{% enddocs %}

{% docs servico_original_subsidio %}
Serviço apurado no subsídio
{% enddocs %}

{% docs km_planejada %}
Distância planejada para o serviço (km)
{% enddocs %}

{% docs data_fim_irk %}
Data final do período para um determinado IRK
{% enddocs %}

{% docs data_inicio %}
Data inicial do período para um determinado IRK
{% enddocs %}

{% docs quantidade_dia_falha_operacional %}
Quantidade consecutiva de dias com falha operacional do ar condicionado
{% enddocs %}

{% docs indicadores_viagem %}
Indicadores para classificação da viagem
{% enddocs %}

{% docs data_verificacao_regularidade %}
Data de verificação da regularidade
{% enddocs %}

{% docs sequencia_integracao %}
Sequência da transação dentro da integração
{% enddocs %}

{% docs hash_cartao %}
Hash identificador do cartão [protegido]
{% enddocs %}

{% docs tipo_transacao_jae %}
Tipo de transação realizada conforme o dado original da Jaé [a primeira perna de integrações são classificadas como tipo Débito e não Integração]
{% enddocs %}

{% docs id_ordem_pagamento_consorcio_operador_dia %}
Identificador único da tabela rj-smtr.br_rj_riodejaneiro_bilhetagem.ordem_pagamento_consorcio_operador_dia
{% enddocs %}

{% docs id_ordem_pagamento_servico_operador_dia %}
Identificador único da tabela rj-smtr.br_rj_riodejaneiro_bilhetagem.ordem_pagamento_servico_operador_dia
{% enddocs %}

{% docs id_ordem_pagamento_consorcio_dia %}
Identificador único da tabela rj-smtr.br_rj_riodejaneiro_bilhetagem.ordem_pagamento_consorcio_dia
{% enddocs %}

{% docs id_ordem_pagamento %}
Identificador único da tabela rj-smtr.br_rj_riodejaneiro_bilhetagem.ordem_pagamento_dia
{% enddocs %}

{% docs valor_pagamento %}
Valor de pagamento da transação
{% enddocs %}

{% docs indicador_temperatura_variacao_veiculo %}
Indicador se houve variação na temperatura transmitida pelo veículo
{% enddocs %}

{% docs indicador_temperatura_transmitida_veiculo %}
Indicador se o veículo transmitiu dados de temperatura
{% enddocs %}

{% docs percentual_viagem_temperatura_pos_tratamento_descartada %}
Percentual de viagens com mais de 50% de registros de temperatura descartados após tratamento estatístico em um dia de operação
{% enddocs %}

{% docs percentual_temperatura_pos_tratamento_descartada %}
Percentual de registros de temperatura descartados após tratamento estatístico em um dia de operação
{% enddocs %}

{% docs indicador_temperatura_descartada_veiculo %}
Indica se percentual_temperatura_pos_tratamento_descartada é maior que 50%
{% enddocs %}

{% docs indicador_viagem_temperatura_descartada_veiculo %}
Indica se percentual_viagem_temperatura_pos_tratamento_descartada é maior que 50%
{% enddocs %}

{% docs estado_equipamento %}
Validador aberto ou fechado no momento da transmissão
{% enddocs %}

{% docs servico_cadastro %}
Nome do serviço consolidado. Para linhas, é o nome curto da linha [ex: 010, 011SN]. E para estações, é o stop_code do GTFS ou o código interno da estação no banco de dados da Jaé. [Caso o registro não exista no GTFS, busca o serviço na base de dados da Jaé]
{% enddocs %}

{% docs descricao_servico_cadastro %}
Nome completo do serviço consolidado. Para linhas é, primariamente o route_long_name da tabela routes do GTFS. Para estações é primariamente a coluna stop_name da tabela stops do GTFS. [Caso o registro não exista no GTFS, busca o serviço na base de dados da Jaé]
{% enddocs %}

{% docs tipo_documento_cliente %}
Tipo do documento do cliente
{% enddocs %}

{% docs documento_cliente %}
Número do documento do cliente
{% enddocs %}

{% docs nome_cliente %}
Nome do cliente
{% enddocs %}

{% docs nome_social_cliente %}
Nome social do cliente
{% enddocs %}

{% docs subtipo_usuario %}
Subtipo do usuário que efetuou a transação sem dados relacionados à saúde [ex.: Ensino Básico Municipal]
{% enddocs %}

{% docs id_cliente_particao %}
Identificador do cliente no tipo inteiro
{% enddocs %}

{% docs id_cre_escola %}
Identificador da Coordenadoria Regional de Educação
{% enddocs %}

{% docs cpf_particao %}
Número do CPF no tipo inteiro
{% enddocs %}

{% docs telefone_cliente %}
Número do telefone do cliente
{% enddocs %}

{% docs datetime_cadastro_cliente %}
Data e hora do cadastro do cliente no sistema da Jaé
{% enddocs %}

{% docs numero_sequencia_endereco_cliente %}
Sequencial do endereço na base da Jaé
{% enddocs %}

{% docs id_cliente_sequencia %}
Identificador único do endereço [concatenação do id_cliente com numero_sequencia_endereco separado por '-']
{% enddocs %}

{% docs tipo_endereco_cliente %}
Residencial ou comercial
{% enddocs %}

{% docs cep %}
Número do CEP
{% enddocs %}

{% docs logradouro %}
Logradouro do endereço
{% enddocs %}

{% docs numero_endereco %}
Número do endereço
{% enddocs %}

{% docs complemento_endereco %}
Complemento do endereço
{% enddocs %}

{% docs bairro %}
Nome do bairro
{% enddocs %}

{% docs cidade %}
Nome da cidade
{% enddocs %}

{% docs uf %}
Sigla da UF
{% enddocs %}

{% docs datetime_inclusao_endereco_jae %}
Data e hora do cadastro do endereco no sistema da Jaé
{% enddocs %}

{% docs datetime_inativacao_endereco_jae %}
Data e hora da inativação do cadastro do endereco no sistema da Jaé
{% enddocs %}

{% docs resolution %}
Resolução do h3
{% enddocs %}

{% docs parent_id %}
Resolução do h3, parent cell
{% enddocs %}

{% docs geometry_wkt %}
Geometria em formato WKT
{% enddocs %}

{% docs data_ordem_servico %}
Data da ordem de serviço
{% enddocs %}

{% docs pico_multa_automatica %}
Período de pico de operação, dividido em 'manhã' [5h-8h] e 'noite' [16h-19h]
{% enddocs %}

{% docs frota_operante_media_multa_automatica %}
Média de veículos em operação na linha no período de pico
{% enddocs %}

{% docs frota_planejada_multa_automatica %}
Frota planejada para a linha no período de pico
{% enddocs %}

{% docs porcentagem_operacao_multa_automatica %}
Razão entre a frota realizada e a frota planejada
{% enddocs %}

{% docs multavel_multa_automatica %}
Indica se a linha é passível de multa por operar com frota abaixo de 80% do planejado
{% enddocs %}

{% docs latitude %}
Valor geográfico da latitude do ponto.
{% enddocs %}

{% docs longitude %}
Valor geográfico da longitude do ponto.
{% enddocs %}

{% docs hora %}
Horário do registro
{% enddocs %}

{% docs datetime_gps %}
Data e hora de geração da transmissão do GPS [GMT-3]
{% enddocs %}