open Devkit

module IndexTypeMappingParamDynamic = struct
  type t = [ `True | `False | `Strict ]
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
