import std/[os, strutils]
import npeg, nimpy

type
  NodeKind = enum
    kNull
    kStop
    kReqStop
    kName
    kValue
    kRealm
    kFreq
    kVariable
  Node = object
    case kind: NodeKind
    of kNull, kStop, kReqStop, kVariable:
      discard
    of kName, kValue, kRealm, kFreq:
      s: string
  Nodes = seq[Node]
  Values = seq[string]
  Facet = object
    name: string
    values: Values
  Request = seq[Facet]
  Query = object
    facets: Request
    requests: seq[Request]
  Context = object
    path: string
    ok: bool
    nodes: Nodes
    query: Query
  Contexts = seq[Context]

proc `$`(facet: Facet): string =
  $facet.name & ": \"" & facet.values.join(",") & "\""

proc `$`(query: Query): string =
  for facet in query.facets:
    result.add $facet & "\n"
  if query.requests.len > 0:
    result.add "requests:\n"
    for request in query.requests:
      result.add "  - " & $request[0] & "\n"
      for facet in request[1..^1]:
        result.add "    " & $facet & "\n"

proc outPath(ctx: Context): string =
  ctx.path.changeFileExt("yaml")

proc writeYaml(ctx: Context) =
  ctx.outPath.writeFile($ctx.query)

grammar "g":
  Eq        <- '='
  Colon     <- ':'
  Comment   <- '#'
  BrOpen    <- '['
  BrClose   <- ']'
  ValueSep  <- ' ' | ',' # Not using `Space` since it matches newline
  Variable  <- "variable"
  Newline   <- '\n' * ?'\r'
  String    <- +(Alnum | '_' | '-' | '*')

let nodeParser = peg("selectionFile", nodes: Nodes):
  empty           <- g.Newline:
    ## consume and skip empty line

  comment         <- g.Comment * *(1 - g.Newline):
    ## consume and skip any line starting with `#`

  values          <- >g.String * *(g.ValueSep * >g.String):
    ## one or more values separated by space or dash
    for i in 1 ..< capture.len:
      nodes.add Node(kind: kValue, s: capture[i].s)

  facet           <- >g.String * g.Eq * values:
    ## basic facet definition: `name=value1,value2`
    nodes.add Node(kind: kName, s: $1)
    nodes.add Node(kind: kStop)

  bracket1        <- g.BrOpen * ?( > g.String * g.Eq) * >g.String * g.BrClose:
    ## single bracket: `variable[...]=...`
    if capture.len == 2:
      ## `[frequency]` case
      nodes.add Node(kind: kFreq, s: $1)
    elif capture.len == 3:
      ## `[name=value]` case
      nodes.add Node(kind: kName, s: $1)
      nodes.add Node(kind: kValue, s: $2)
      nodes.add Node(kind: kReqStop)
    else:
      raise newException(ValueError, "Unexpected number of arguments: " & $0)

  bracket2        <- (g.BrOpen * >g.String * g.BrClose)[2]:
    ## double bracket: `variable[realm][frequency]=...`
    nodes.add Node(kind: kRealm, s: $1)
    nodes.add Node(kind: kFreq, s: $2)

  variable        <- g.Variable * (bracket2 | bracket1) * g.Eq * values:
    ## variable definition with one/two conditions `[]`, triggers new requests branch
    ##   check is done for double brackets first, otherwise
    ##   single brackets can match when it should not.
    nodes.add Node(kind: kVariable)
    nodes.add Node(kind: kStop)

  line            <- empty | comment | facet | variable:
    ## a line can only be one of the following
    discard
  selectionFile   <- *(line * @(g.Newline)) * (!1 | g.Newline):
    ## a selection file is a sequence of (line + newline) until all characters are consumed
    discard

iterator items(contexts: var Contexts): var Context =
  for i in contexts.low .. contexts.high:
    yield contexts[i]

iterator items(requests: var seq[Request]): var Request =
  for i in requests.low .. requests.high:
    yield requests[i]

iterator items(request: var Request): var Facet =
  for i in request.low .. request.high:
    yield request[i]

proc parseNodes(ctx: var Context) =
  let
    content = readFile(ctx.path)
  ctx.ok = nodeParser.match(content, ctx.nodes).ok

proc next(nodes: var Nodes): Node =
  result = nodes[0]
  nodes = nodes[1..^1]

proc hasCmip6(ctx: Context): bool =
  for facet in ctx.query.facets:
    if facet.name in ["project", "mip_era"] and "CMIP6" in facet.values:
      return true
  false

proc fixFacetNames(r: var Request) =
  for facet in r:
    facet.name =
      case facet.name
      of "frequency": "table_id"
      of "variable": "variable_id"
      else: facet.name

proc postProcess(ctx: var Context) =
  if ctx.hasCmip6:
    fixFacetNames ctx.query.facets
    for request in ctx.query.requests:
      fixFacetNames request

proc translate(ctx: var Context, dumpKeys: openArray[string]) {.raises: [ValueError].} =
  var
    nodes = ctx.nodes
    acc: Facet
    req: Request
  while nodes.len > 0:
    let node = next nodes
    case node.kind
    of kNull:
      raise newException(ValueError, "Unexpected null node")
    of kStop:
      if req.len > 0:
        ctx.query.requests.add req
        req = @[]
      elif acc.name.len > 0:
        ctx.query.facets.add acc
      acc = Facet()
    of kName:
      acc.name = node.s
      if acc.name in dumpKeys:
        acc = Facet()
    of kValue:
      acc.values.add node.s
    of kReqStop:
      req.add acc
      acc = Facet()
    of kRealm:
      req.add Facet(name: "realm", values: @[node.s])
    of kFreq:
      req.add Facet(name: "frequency", values: @[node.s])
    of kVariable:
      acc.name = "variable"
      req.add acc
      acc = Facet()

proc translate_selection_file(paths: seq[string], dumpKeys: seq[string] = @[]) {.exportpy.} =
  var contexts = newSeq[Context](paths.len)
  for i, path in paths:
    contexts[i].path = path
  for ctx in contexts:
    parseNodes ctx
    if not ctx.ok:
      echo "ERROR: ", ctx.path
    else:
      ctx.translate(dumpKeys)
      postProcess ctx
      when defined(print):
        for node in nodes:
          echo node
        echo ""
        echo $selection
      if ctx.query.facets.len == 0 and ctx.query.requests.len == 0:
        echo "Empty query file: ", ctx.outPath
      else:
        writeYaml ctx

when defined(cli):
  import cligen; dispatch translate_selection_file
