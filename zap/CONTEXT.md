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

O passado de UM chat, depois, pede-se ao celular: `BuildHistorySyncRequest` +
`SendPeerMessage` (`HISTORY_SYNC_ON_DEMAND`, peer data operation), e a resposta
volta minutos depois como `events.HistorySync` do tipo `ON_DEMAND`. É isso que
o `follow` usa (veja "Seguir é liberar, buscar o passado e sincronizar"). O
pedido de histórico da CONTA inteira (`FullHistorySyncOnDemandRequest`) existe
no protocolo, mas o whatsmeow não o embrulha e o zap não o usa.

## Nada de um chat sem permissão

**Um chat que não está na allowlist não é guardado, não sai no `tail`, não
chega a um hook e não vai a nenhum backend — e quem decide é uma porta só,
`Policy.Admit`.** O `store.db`, o barramento do `tail` e o backend ficam atrás
dela; `remember` ainda confere a lista por conta própria, para que nenhum
chamador novo consiga esquecer a regra. Sem `allow.toml`, o zap pareia e
conecta, mas não escuta ninguém. Evento de conta (conectou, desconectou, chegou
um pacote de histórico) não é de ninguém e passa.

O pacote de histórico do pareamento traz TODOS os chats do celular: o `absorb`
descarta o que não é de chat liberado, sem ler. A allowlist é relida quando o
arquivo muda (edição à mão vale com o daemon no ar), e um arquivo quebrado
mantém a lista que já estava carregada: um erro de digitação nunca abre nem
fecha a porta. Um mesmo contato chega como JID de telefone ou como `@lid`;
`follow` grava os dois (`alt`) e `NormJID` tira o sufixo de dispositivo.

Nada de texto de mensagem em log nem em erro: `zap log` troca o `text` do corpo
pelo tamanho, e o que vai a stderr são contagens e a classe do erro. Não há
telemetria nem chamada de rede além do WhatsApp e do backend configurado.

## Seguir é liberar, buscar o passado e sincronizar

**`zap follow` libera o chat, pede ao celular o passado dele e, dali em diante,
sincroniza cada mensagem nova; `unfollow` desfaz os três.** Como o whatsmeow
deixa pedir o histórico de um chat só, depois do pareamento, o zap não
guarda nada de chat não liberado — nem cifrado.

O pedido precisa de uma ÂNCORA: uma mensagem real do chat (id, de quem, hora).
O celular devolve até `count` mensagens imediatamente ANTES dela — a âncora em si
não vem — e a recomendação do código é 50 por pedido. O zap guarda, só na
memória e nunca em disco, a mensagem mais nova que viu de cada chat (id, sentido
e hora; nenhum texto) vinda do pacote do pareamento e do tráfego ao vivo. Sem
âncora (chat quieto e o zap reiniciou), o `follow` deixa o chat `pending` e a
primeira mensagem ao vivo vira a âncora. A mensagem-âncora, cujo corpo nunca foi
guardado, não entra no passado.

Cada resposta `ON_DEMAND` traz, por chat, o `EndOfHistoryTransferType`: é ele que
diz se o celular ainda tem mensagens mais antigas. O zap pede a próxima página
(ancorada na mais antiga recebida) até `--max` mensagens, até `--days`, ou até o
celular dizer que acabou; sem resposta em 3 minutos, o backfill fica `stalled`
e um novo `follow` o retoma.

Limites, lidos no código e não exercitados contra um celular: o celular
principal precisa estar ligado e com internet; só vem o que ele ainda guarda
(o que foi apagado ali ou é anterior a uma restauração não volta); mídia não
vem, só texto e metadados. O passado vai ao backend com `backfill: true`, e o
`Nats-Msg-Id` (`conta/chat/id`) faz repetir um backfill ser inofensivo.

`unfollow` apaga as mensagens e os áudios locais do chat. O que já foi
publicado num backend é do dono do backend: a retenção dele (`max_age`) é a
defesa.

## Um backend, escolhido num arquivo

**`config.toml` escolhe UM backend (`ndjson` ou `nats`) atrás de uma interface
de dois métodos, `Sink`; o zap lê esse arquivo e nunca o reescreve.** O que o zap
reescreve (quem é seguido) mora em outro arquivo, a allowlist, porque um arquivo
que um programa reescreve perde os comentários de quem o escreveu. Chave
desconhecida no config é erro: um erro de digitação numa configuração de
privacidade não pode ser ignorado em silêncio.

O backend publica pelo `Outbox`: uma fila limitada e um worker, porque o handler
do whatsmeow roda numa goroutine só e um broker lento não pode travá-la. Fila
cheia espera 2 s e descarta, contando (sem escrever a mensagem). No NATS, o
stream e os subjects são do dono; o zap confere na partida que o stream captura
`<prefix>.message.received` e `<prefix>.chat.followed` e recusa subir se não
captura. O token nunca vai ao arquivo: `token_env` é o NOME da variável.

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

O `--fake` nasce atrás da MESMA porta: nada de chat é escutado até um `zap
follow`, e `zap fake history` dá ao celular de mentira um passado (uma mensagem
por hora) que só um `follow` consegue pedir, em páginas de 50, como o real.

O que `--fake` deliberadamente NÃO simula: persistência entre reinícios (a
memória morre com o processo), um bundle de histórico respondendo `SyncChat`
(a resposta é sempre só o recibo, como no daemon real), confirmações de
entrega/leitura, o sweeper de `session_closed`, mídia além de áudio, e
injeção de latência ou falha. `zap fake contact|group|inbound|audio` cobre o
que um consumidor precisa para montar um cenário: um contato, um grupo, uma
mensagem chegando, um áudio (com `--text` pulando o whisper de verdade).
