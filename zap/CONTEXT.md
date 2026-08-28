# zap

## Um binário, dois papéis

**`zap serve` é o processo que segura todos os números; todo outro verbo é
cliente HTTP do unix socket que ele abre.** Protocolo próprio não foi escrito:
`net.Listen("unix", …)` + `net/http` já dá roteamento, status e erro, e
`curl --unix-socket /tmp/zap/zap.sock http://zap/accounts` funciona sem cliente.

Streams (`/pair`, `/tail`) são ndjson com flush por linha — SSE não paga o
próprio custo aqui, e `zap tail | jq` continua vivo em vez de encher buffer.

O socket é `0600` e nunca é roubado: se já houver daemon vivo no caminho, o
segundo recusa. E o caminho tem teto de ~100 bytes, senão o kernel devolve um
`invalid argument` que não explica nada.

## Modos de unix

**Numa tty o zap fala com humano; em pipe ele emite só ndjson, um registro por
linha, e todo o resto vai para stderr.** `zap accounts | jq` não deve nunca ter
que parsear tabela, e `zap send … 2>/dev/null` não deve engolir dado.

O corpo da mensagem vem dos argumentos ou do stdin, então
`cat aviso.txt | zap send --to +55…` funciona como qualquer filtro. Saída 0 é
sucesso, 1 é falha de execução, 2 é uso errado.

O binário é compilado: não tem shebang. Global é symlink em `~/.local/bin/zap`,
que já está no PATH — `go install .` sozinho não basta, `~/go/bin` está fora.

## O evento carrega a conta

**Todo `Event` sai com o campo `account`, e `--from` é obrigatório em `send`
quando há mais de um número pareado.** O handler do whatsmeow não diz de qual
client veio o evento; sem carimbar na origem, duas caixas de entrada viram uma
sem nenhum erro na tela.

Adivinhar o remetente não é recuperável — a mensagem já saiu pelo número errado.
Por isso `pick()` prefere falhar a escolher.

## Nunca pergunte ao client se ele está conectado

**O estado de conexão é rastreado por evento (`events.Connected` /
`Disconnected`) num `atomic.Bool`, nunca por `cli.IsConnected()`.** `Connect`
segura o `socketLock` durante todo o handshake Noise — até 20s — e qualquer
chamada que pegue o mesmo lock trava junto.

O custo já foi pago: no `bench/`, um sampler que chamava `IsConnected()` travou
o processo filho inteiro e o pai esperou o timeout de prontidão até o fim.

## Histórico só se pede na hora de parear

**O telefone despeja o histórico num bundle no momento do link, e o tamanho
desse bundle é decidido ali — `zap serve --history-days 365` só vale para
números pareados DEPOIS dessa flag.** Widen depois não traz nada: os limites
viajam dentro do payload de pareamento.

O whatsmeow guarda sessão, nunca corpo de mensagem: o `events.HistorySync`
passa e some. Por isso existe a tabela `messages` no mesmo `store.db`, escrita
pelo `absorb` (bundle) e pelo handler (mensagem viva) — e também pelo `Send`,
porque envio próprio NÃO volta como evento no aparelho que enviou.

Quem quer o histórico antigo de um número já pareado tem um caminho só:
despareá-lo e parear de novo com a janela maior. Não há API de "me manda tudo
de novo" fora do pedido por chat (`BuildHistorySyncRequest`), que ainda não foi
construído aqui.

## Sessão é view, não tabela

**Uma sessão é um trecho de conversa com um contato, cortado onde o silêncio
passou do gap — 24h por padrão — e ela é DERIVADA das mensagens guardadas a
cada pergunta.** Nada para migrar quando o gap muda: `--gap 4h` e `--gap 24h`
leem a mesma tabela e devolvem cortes diferentes.

`splitSessions` caminha do mais antigo para o mais novo dentro de cada chat, e
é a única lógica de verdade dessa view — por isso é ela que tem teste próprio.

`zap sessions --number +55… --count` responde só o número, para caber em
`$(...)`. Sem `--count`, uma linha por sessão com início, fim e quantas
mensagens de cada lado.

## O log é a integração

**`zap log` transmite, em ndjson e em tempo real, toda requisição que o daemon
atende — método, path, status, milissegundos e o corpo.** É o ponto de
integração para qualquer consumidor: `zap log | jq` vê o sistema trabalhar sem
ter sido construído dentro dele.

`/log` e `/tail` não se registram: um stream que loga o próprio leitor nunca
fica em silêncio. E o fanout descarta para leitor lento — `zap log` travado
jamais trava o daemon que ele observa.

## Qual número age

**A ordem é `--from`, depois `zap use`, depois o daemon — e o daemon só decide
quando existe exatamente um número pareado.** O default mora num arquivo em
`$ZAP_HOME/default`, não no daemon: de qual número VOCÊ manda é propriedade do
seu shell, não do processo que segura todos.

## O que está verificado

**`go test ./...` sobe o daemon num socket real com backend stub e exercita
accounts, send, pair e tail, mais a recusa do segundo daemon.** Roda em ~0.4s e
não precisa de telefone.

Verificado à mão contra o backend real: `serve` com store vazio, `accounts`,
`send` sem conta, `curl` direto no socket, segundo daemon recusado.

NÃO verificado, porque exige um número de verdade: o QR do `zap pair`, o envio
de mensagem e o formato dos eventos que chegam no `zap tail`.

## `--fake` é o MESMO daemon, sem telefone

**`zap serve --fake` sobe o mesmo `Daemon`, as mesmas rotas, o mesmo wire —
só troca o `Backend` de `*WhatsApp` para `*Fake` e registra uma rota a mais,
`POST /fake`.** Um consumidor (o runtime de agente em TypeScript, um CI) fala
com ele exatamente como falaria com o daemon real: mesmo socket unix, mesmo
JSON, mesmo `zap tail`. `POST /fake` existe em TODO daemon, não só no fake —
contra um daemon real ela responde a frase "this daemon is real…", nunca um
404 mudo, porque uma asserção de tipo (`d.Backend.(*Fake)`) é mais barata e
mais honesta que uma interface com uma implementação só.

`Fake` guarda tudo em memória — contas, contatos, grupos, mensagens — e nunca
abre sqlite. Não existe `store.db` para um `--fake` clobberar, porque não
existe arquivo nenhum. `ZAP_HOME` continua obrigatório: sem ele, ou com
`--socket` ainda no default, `zap serve --fake` recusa a subir, porque o
socket real (`~/.zap/zap.sock`) nunca pode ser roubado por um processo de
teste.

O que `--fake` deliberadamente NÃO simula: persistência entre reinícios (a
memória morre com o processo), um bundle de histórico respondendo `SyncChat`
(a resposta é sempre só o recibo, como no daemon real), confirmações de
entrega/leitura, o sweeper de `session_closed`, mídia além de áudio, e
injeção de latência ou falha. `zap fake contact|group|inbound|audio` cobre o
que um consumidor precisa para montar um cenário: um contato, um grupo, uma
mensagem chegando, um áudio (com `--text` pulando o whisper de verdade).
