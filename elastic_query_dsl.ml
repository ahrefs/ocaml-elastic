open ExtLib

type term = Yojson.Safe.t

type query = Yojson.Safe.t

type top_query = Yojson.Safe.t

let bool_val x = `Bool x

let int_val x = `Int x

let int64_val x = `Intlit (Int64.to_string x)

let float_val x = `Float x

let string_val x = `String x

let list_val f l = `List (List.map f l)

let int64_list l = list_val int64_val l

let bool_list l = list_val bool_val l

let int_list l = list_val int_val l

let float_list l = list_val float_val l

let string_list l = list_val string_val l

let filter_kv' name value = `Assoc [ name, value ]

let filter_kv (name : string) (field : string) value : Yojson.Safe.t = filter_kv' name (`Assoc [ field, value ])

let filter_term field value = filter_kv "term" field value

let filter_terms field values = filter_kv "terms" field values

let filter_string name ~field s = filter_kv name field (string_val s)

let filter_prefix = filter_string "prefix"

let filter_ids (values : 'a list) = filter_kv "ids" "values" (`List values)

let filter_range' field terms = filter_kv "range" field (`Assoc terms)

let filter_regexp = filter_string "regexp"

let filter_wildcard = filter_string "wildcard"

let filter_operator' ?field operator_field operator query : Yojson.Safe.t =
  let field_entry = field |> Option.map_default (fun f -> [ "fields", `List [ `String (f : string) ] ]) [] in
  let operator_entry =
    match operator with
    | None -> []
    | Some operator ->
      let operator =
        match operator with
        | `And -> "and"
        | `Or -> "or"
      in
      [ operator_field, `String operator ]
  in
  `Assoc ([ "query", string_val query ] @ field_entry @ operator_entry)

let filter_operator ?field value = filter_operator' ?field "operator" value

let filter_default_operator ?field value = filter_operator' ?field "default_operator" value

let filter_match ~field ?operator value =
  let value = filter_operator operator value in
  filter_kv "match" field value

let filter_match_phrase = filter_string "match_phrase"

let filter_range field op value = filter_range' field [ op, value ]

let filter_op op l = `Assoc [ op, `List l ]

let filter_bool ?(filter : query list option) ?must ?must_not ?should ?(minimum_should_match : int option) () =
  let bool =
    List.filter_map
      (function
        | _, Some [] -> None
        | k, Some v -> Some (k, `List v)
        | _ -> None
        )
      [ "filter", filter; "must", must; "must_not", must_not; "should", should ]
  in
  let bool =
    match minimum_should_match with
    | Some v -> ("minimum_should_match", int_val v) :: bool
    | None -> bool
  in
  `Assoc [ "bool", `Assoc bool ]

let filter_must l = filter_bool ~must:l ()

let filter_and l = filter_bool ~filter:l ()

let filter_or l = filter_bool ~should:l ()

let filter_not x = `Assoc [ "bool", `Assoc [ "must_not", x ] ]

let filter_exists field = filter_kv "exists" "field" (`String field)

let filter_missing_or field l = `Assoc [ "bool", filter_op "should" (filter_not (filter_exists field) :: l) ]

let query_string ?field ?default_operator s =
  let value = filter_default_operator ?field default_operator s in
  `Assoc [ "query_string", value ]

let nested path query = `Assoc [ "nested", `Assoc [ "path", `String path; "query", query ] ]

let match_phrase_prefix field s max_expansions =
  filter_kv "match_phrase_prefix" field (`Assoc [ "query", `String s; "max_expansions", `Int max_expansions ])

let match_all = `Assoc [ "match_all", `Assoc [] ]

let filter_and_match_all l =
  match l with
  | [] -> match_all
  | l -> filter_and l

let query_to_json x = x

let json_of_query x = x

let rec basic_json_of_query = function
  | (`Bool _ | `Float _ | `Int _ | `String _ | `Null) as x -> x
  | `Intlit x -> `String x
  | `List l -> `List (List.map basic_json_of_query l)
  | `Assoc l -> `Assoc (List.map (fun (k, v) -> k, basic_json_of_query v) l)
  | `Tuple _ | `Variant _ -> assert false

(* these are never produced by DSL combinators *)

let make_top_query' ?(args = []) filter = `Assoc (("query", filter) :: args)

let make_top_query ?(args = []) filters =
  let filter = filter_and_match_all filters in
  `Assoc (("query", filter) :: args)

let top_query_to_json x = x

let top_query_to_string x = Yojson.Safe.to_string x

let empty_top_query = `Assoc []

let basic_json_assoc_of_filters_agg (filters : (string * query) list) =
  `Assoc (List.map (fun (name, filter) -> name, Yojson.Safe.to_basic (query_to_json filter)) filters)

module Unsafe = struct
  let top_query_of_json x = x

  let query_of_json x = x
end
