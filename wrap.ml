open Devkit

module IndexTypeMappingParamDynamic = struct
  type t =
    [ `True
    | `False
    | `Strict
    ]

  let wrap = function
    | `Bool true | `String "true" -> `True
    | `Bool false | `String "false" -> `False
    | `String "strict" -> `Strict
    | x -> Exn.fail "index_type_mapping_param_dynamic %s" (Yojson.Safe.to_string x)

  let unwrap = function
    | `True -> `Bool true
    | `False -> `Bool false
    | `Strict -> `String "strict"
end

module IndexTypeMappingMetaList = struct
  type t =
    [ `False
    | `True
    | `Sometimes
    ]

  let wrap = function
    | `Bool true | `String "true" -> `True
    | `Bool false | `String "false" -> `False
    | `String "sometimes" -> `Sometimes
    | x -> Exn.fail "index_type_mapping_meta_list %s" (Yojson.Basic.to_string x)

  let unwrap = function
    | `True -> `Bool true
    | `False -> `Bool false
    | `Sometimes -> `String "sometimes"
end

module Total = struct
  type t = {
    value : int;
    relation : string;
  }

  (* We never unwrap it anyway ? *)
  let unwrap x = `Int x.value

  let wrap = function
    | `Int x -> { value = x; relation = "eq" }
    | x ->
    try
      {
        value = Yojson.Basic.Util.(member "value" x |> to_int);
        relation = Yojson.Basic.Util.(member "relation" x |> to_string);
      }
    with _ -> Exn.fail "total %s" (Yojson.Basic.to_string x)

  let sprint () x = Printf.sprintf "%s %d" x.relation x.value
end
