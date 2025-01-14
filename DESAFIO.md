<style>
r { color: Red }
o { color: Orange }
g { color: Green }
</style>

# Teste Prático de PySpark com Orientação a Objetos (POO)

## Cenário

Você trabalha como Engenheiro de Dados em uma empresa com Marketplace para vendas de passagens de ônibus. A cada pesquisa que os usuários fazem na plataforma são gerados eventos JSON detalhados que contêm informações sobre várias opções de viagens. Seu objetivo é criar um pipeline robusto, usando conceitos de Orientação a Objetos, para processar e extrair insights desses dados.

## Objetivo:

Projetar uma solução em PySpark para processar eventos no formato JSON, adotando a Programação Orientada a Objetos (POO) para estruturar a lógica de transformação, filtragem e agregação de dados. A solução deve ser capaz de gerar insights valiosos e salvar os resultados de forma eficiente. Além disso, deve ser compatível para execução em qualquer plataforma, como Kubernetes, Linux, ou outras infraestruturas semelhantes

## Descrição dos Dados:

Os dados são recebidos no seguinte formato:
- **Nível Superior (Evento):**
  - <g>id</g>: Identificador único do evento.
  - <g>timestamp</g>: Horário do evento.
  - <g>live</g>: Indica se o evento é em tempo real (<o>true</o>) ou histórico (<o>false</o>).
  - <g>organic</g>: Indica se a busca foi orgânica (<o>true</o>) ou (<o>false</o>).
  - <g>data</g>: JSON contendo um array chamado <o>searchItemsList</o>.

- **Nível Inferior (searchItemsList):** Cada item dentro de searchItemsList
contém os seguintes campos:

  - **Detalhes da viagem:**
    - <g>departureDate, departureHour</g>: Data e hora de partida.
    - <g>arrivalDate, arrivalHour</g>: Data e hora de chegada.
    - <g>originCity, originState</g>: Cidade e estado de origem.
    - <g>destinationCity, destinationState</g>: Cidade e estado de destino.
    - <g>route</g>: (Criada no pipeline) Combinação <o>Origem -> Destino</o>.
  
  - **Detalhes comerciais:**
    - <g>price</g> Preço da viagem.
    -  <g>serviceClass</g> Classe de serviço (e.g., *<o>"LEITO"</o>*, *<o>"EXECUTIVO"</o>*).
    -  <g>availableSeats</g> Assentos disponíveis.
    -  <g>travelCompanyName</g> Nome da companhia de viagem.
  
  - **Metadados:**
    - <g>distributorIdentifier</g>: Identificador do distribuidor.
    - <g>groupId</g>: Identificador do grupo.
  
  ```json
  {
    "id": "747cf833-1009-4648-94f1-358d44e159aa",
    "timestamp": "2024-11-28T13:12:27.322211538",
    "data": {
      "searchItemsList": [
        {
          "travelPrice": "180.50",
          "travelCompanyId": 101,
          "travelCompanyName": "Rapido Vermelho",
          "distributorIdentifier": "rotaonline",
          "departureDate": "2024-12-19",
          "departureHour": "10:00",
          "arrivalDate": "2024-12-19",
          "arrivalHour": "18:00",
          "originId": 1,
          "originCity": "RIO DE JANEIRO",
          "originState": "RJ",
          "destinationId": 2,
          "destinationCity": "SÃO PAULO",
          "destinationState": "SP",
          "serviceClass": "EXECUTIVO",
          "serviceCode": "RJSP-100",
          "availableSeats": 12,
          "price": 180.50,
          "referencePrice": null,
          "originalPrice": 200.00,
          "discountPercentageApplied": 9.75,
          "tripId": null,
          "groupId": "RJSPS"
        }
      ],
      "platform": null,
      "clientId": null
    },
    "live": true,
    "organic": true
  }
  ```

## Tarefas:
### Parte 1: Criar uma Estrutura de Classes em POO


1. **Classe <g>EventProcessor</g>:** Deve conter métodos para:
    - Ler o JSON em um DataFrame PySpark.
    - Explodir o array <g>searchItemsList</g> para normalizar os dados.
    - Criar colunas derivadas:
      - <g>departure_datetime</g>: Combinação de <o>departureDate</o> e
<o>departureHour</o>.
      - <g>arrival_datetime</g>: Combinação de <o>arrivalDate</o> e
<o>arrivalHour</o>.
      - <g>route</g>: Combinação <g>originCity</g> e <g>destinationCity</g>.
    - Filtrar dados:
      - Viagens futuras (baseadas no <g>departure_datetime</g>).
      - Viagens com `availableSeats > 0`.


2. **Classe <g>Aggregator</g>**: Deve conter métodos para:
    - Calcular o preço médio por rota e classe de serviço.
    - Determinar o total de assentos disponíveis por rota e
companhia.
    - Identificar a rota mais popular por companhia de viagem.


3. **Classe Writer:** Deve conter métodos para:
    - Salvar os dados processados em formato Parquet.
    - Garantir que os arquivos sejam particionados por <g>originState</g> e <g>destinationState</g>.


### Parte 2: Implementar as Transformações
Após criar as classes, implemente os seguintes métodos:
- <g>EventProcessor.process_events()</g>:
  - Leia o JSON.
  - Normalize os dados.
  - Retorne o DataFrame processado.
- <g>Aggregator.aggregate_data()</g>:
  - Receba o DataFrame processado.
  - Gere as agregações solicitadas.
  - Retorne um DataFrame com os insights.
- <g>Writer.write_data()</g>:
  - Salve os dados processados em Parquet.


### Parte 3: Testar o Pipeline
Implemente uma função principal (<g>main</g>) que:
1. Instancie as classes <g>EventProcessor</g>, <g>Aggregator</g> e <g>Writer</g>.
2. Leia os dados de um arquivo JSON fornecido.
3. Realize o processamento, as agregações e salve os resultados.

## Entrega Final:
- Repositório Git com a estruturação completa da solução, incluindo todos os arquivos de código, dependências e configurações necessárias.
- Código organizado e modularizado, utilizando boas práticas de Programação Orientada a Objetos (POO)
- Resultados salvos em formato Parquet.
- Um README explicando detalhadamente a solução, incluindo a arquitetura, instruções de implantação e instruções de testes.