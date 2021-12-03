type term

type query

type top_query

val bool_val : bool -> term

val int_val : int -> term

val int64_val : Int64.t -> term

val float_val : float -> term

val string_val : string -> term

val list_val : ('a -> term) -> 'a list -> term

val int64_list : Int64.t list -> term

val bool_list : bool list -> term

val int_list : int list -> term

val float_list : float list -> term

val string_list : string list -> term

(** NB term is usually split on word boundaries (tokenized), will not work for multi-word query *)
val filter_term : string -> term -> query

val filter_terms : string -> term -> query

val filter_prefix : field:string -> string -> query

val filter_ids : term list -> query

val filter_range' : string -> (string * term) list -> query

val filter_range : string -> string -> term -> query

val filter_regexp : field:string -> string -> query

(** NB wildcard on analyzed fields will only match per-term, use wildcard on non-analyzed subfield for multi-word grepping *)
val filter_wildcard : field:string -> string -> query

(** NB match on analyzed field doesn't care about word order, use [filter_match_phrase] for exact match on fixed string *)
val filter_match : field:string -> ?operator:[ `And | `Or ] -> string -> query

val filter_match_phrase : field:string -> string -> query

val filter_bool
  :  ?filter:query list ->
  ?must:query list ->
  ?must_not:query list ->
  ?should:query list ->
  ?minimum_should_match:int ->
  unit ->
  query

val filter_must : query list -> query

val filter_and : query list -> query

val filter_or : query list -> query

val filter_not : query -> query

val filter_exists : string -> query

val filter_missing_or : string -> query list -> query

val query_string : ?field:string -> ?default_operator:[ `And | `Or ] -> string -> query

val nested : string -> query -> query

val match_phrase_prefix : string -> string -> int -> query

val match_all : query

val query_to_json : query -> Yojson.Safe.t [@@deprecated "use json_of_query"]

val json_of_query : query -> Yojson.Safe.t

val basic_json_of_query : query -> Yojson.Basic.t

val make_top_query' : ?args:(string * Yojson.Safe.t) list -> query -> top_query

val make_top_query : ?args:(string * Yojson.Safe.t) list -> query list -> top_query

val empty_top_query : top_query

val top_query_to_json : top_query -> Yojson.Safe.t

val top_query_to_string : top_query -> string

val basic_json_assoc_of_filters_agg : (string * query) list -> [> `Assoc of (string * Yojson.Basic.t) list ]

module Unsafe : sig
  val query_of_json : Yojson.Safe.t -> query

  val top_query_of_json : Yojson.Safe.t -> top_query
end
