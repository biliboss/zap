# whatsapp

## Três módulos, e a dependência só aponta para um lado

**`zap/` é o produto, `bench/` é a medição que o justifica, `tests/` é a suíte —
cada um com o seu `go.mod`.** A suíte é módulo separado de propósito: ela importa
`zap`, e `zap` não consegue importá-la de volta.

```
whatsapp/
 ├ justfile     toda tarefa que tem nome
 ├ zap/         module zap    — main.go (CLI) + core/ (biblioteca)
 ├ bench/       module bench  — o footprint medido
 └ tests/       module tests  — replace zap => ../zap
```

O `main.go` ficou fino de propósito: a lógica mora em `zap/core` porque teste em
outro diretório só compila importando um pacote, e `package main` não se importa.

## Um processo, muitos números

**Vários números moram como vários `whatsmeow.Client` dentro de UM processo Go,
nunca como um processo por número.** Medido em `bench/`: 87 KB e duas goroutines
por número contra 15.9 MB por processo — 180× de diferença.

O que quebra ao ignorar isso não é a RAM, é a máquina: mil processos projetam
12.4 GB numa box de 16 GB, e aí a medição — e a produção — passam a medir swap.
Como esse número foi obtido está em `bench/CONTEXT.md`; o que ele obriga no
código está em `zap/CONTEXT.md`.

## `just` é a porta

**Toda tarefa que vale ser repetida tem um nome no `justfile`; comando
redigitado de memória é comando que diverge.** `just` sozinho lista tudo.

| pergunta | receita |
| --- | --- |
| está tudo verde? | `just check` |
| quero rodar sem telefone | `just fake` e `just fake-seed` |
| quanto custa mil números? | `just bench` |
| onde uma coisa se conecta na outra? | `just ask "…"`, `just link A B` |

## O código é o doc, e o outline é o desenho

**Este arquivo guarda POR QUE as coisas são assim; o que elas fazem se lê no
código.** Um parágrafo descrevendo comportamento mente no primeiro refactor,
porque ninguém revisa prosa.

`just outline` imprime os símbolos de topo de cada arquivo Go. Em `main.go` são
trinta `cmd<Verbo>` — o arquivo É a lista de verbos, e nenhum doc precisa dizer
isso. Um outline que lê como pilha é sinal de arquivo fazendo duas coisas.

O `CONTEXT.md` mais próximo vence: um filho especializa ou acrescenta, nunca
repete o pai.

## É um repo público ao lado de um telefone real

**Nada que toque uma conversa de verdade entra no git.** O store fica em
`~/.zap/`, fora da árvore, e `*.db`, `zap.log` e `media/` estão barrados no
`.gitignore` — 46 mil mensagens reais moram a um `git add -A` de distância.

Número real em exemplo é PII, não documentação. Dois já vazaram para o texto de
ajuda e foram trocados por `+5511900000000`; a autoria dos commits usa o endereço
`noreply` do GitHub. Antes de publicar, procure por número, e-mail e `/Users/`.
