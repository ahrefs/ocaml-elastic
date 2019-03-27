
module Yojson = struct

  module Make(T : sig
    type t
    type lexer_state
    val write_json : Bi_outbuf.t -> t -> unit
    val read_json : lexer_state -> Lexing.lexbuf -> t
    val validate_json : 'path -> t -> 'error option
  end) = struct
    type t = T.t
    let write_t = T.write_json
    let read_t = T.read_json
    let validate_t = T.validate_json
  end

  module Basic = Make(Yojson.Basic)

  module Safe = Make(Yojson.Safe)

  module Raw = Make(Yojson.Raw)

end
