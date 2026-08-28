# whatsapp

## Um processo, muitos números

**Vários números do WhatsApp moram como vários `whatsmeow.Client` dentro de UM
processo Go, nunca como um processo por número.** Medido em `bench/`: 1000
clients conectados custam 100 MB de RSS, 2 goroutines cada e 0% de CPU ocioso,
contra 1.6 GB só para 100 processos de um client — 87 KB por número contra 15.9
MB, uma diferença de 180×.

O que quebra quando se ignora isso não é a RAM, é a máquina: mil processos
projetam 12.4 GB numa box de 16 GB, e aí o benchmark — e a produção — passam a
medir swap. O `bench/` recusa esse caso por orçamento em vez de derrubar o host.

O preço de morar junto é um só, e é de código: o handler de eventos não diz de
qual client veio o evento. Feche o `cli` no closure ou carregue `cli.Store.ID`
junto, senão as caixas de entrada se misturam sem nenhum erro na tela.

## Como se checa

**`cd bench && go run . -ladder 1,10,100,1000` regrava `results.csv`.** O método,
o servidor falso e o que o número não cobre estão em `bench/CONTEXT.md` — não
repita aqui.
