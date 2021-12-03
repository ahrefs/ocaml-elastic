open Devkit
open ExtLib
open Printf
module CC = Cache.Count

let log = Log.from "elastic"

let rest_total_hits_as_int = "rest_total_hits_as_int"

let es7_compat_args' = rest_total_hits_as_int, "true"

let es7_compat_args = [ es7_compat_args' ]

let default_doc_type = "_doc"

let default_kind kind = Option.default default_doc_type kind

(** ES limit for text fields *)
let elasticsearch_text_limit = 32766

(** ES limit for HTTP content length is 104857600 *)
let elasticsearch_http_limit = 16_777_216

include Elastic_intf

(** Host selection *)
module Make_host (A : Host_array) : Hosts = struct
  type state = unit

  let hosts = A.hosts

  let new_request = id

  let random_host () =
    match hosts with
    | [||] -> Exn.fail "no hosts (use -host, -cluster, or initialize hosts in the code)"
    | hosts -> Action.array_random_exn hosts
end

module Make_mutable_host (A : Host_array) : Mutable_hosts = struct
  type state = unit

  let hosts = ref A.hosts

  let new_request = id

  let random_host () =
    match !hosts with
    | [||] -> Exn.fail "no hosts (use -host, -cluster, or initialize hosts in the code)"
    | hosts -> Action.array_random_exn hosts
end

module T = struct
  (** field attributes for esgg *)
  type attr =
    [ `List  (** field is a list *)
    | `ListSometimes  (** field is a list or a scalar value *)
    | `Multi [@deprecated "please use `List instead"]  (** field is a list *)
    | `Optional  (** field is optional *)
    | `NotOptional  (** field is not optional (overrides FieldsDefaultOptional on parent object) *)
    | `Ignore  (** field should be ignored *)
    | `FieldsDefaultOptional  (** object or nested fields are optional *)
    | `TypeOverride of Elastic_t.index_type_mapping_field_type  (** use the specified type instead of the ES type *)
    | `TypeRepr of [ `Int64 ]
      (** use the specified type representation instead of the default (<ocaml repr="type"> in atd) *)
    ]

  type field = {
    meta : Elastic_t.index_type_mapping_meta;
    field : Elastic_t.index_type_mapping_field_base;
    properties : (string * field) list;
    fields : (string * field) list;
  }

  type mapping = {
    meta : Elastic_t.index_type_mapping_meta;
    mapping : Elastic_t.index_type_mapping_base;
    properties : (string * field) list;
  }

  let default_meta =
    Elastic_t.
      {
        fields_default_optional = None;
        ignore = false;
        list = `False;
        multi = false;
        optional = None;
        type_override = None;
        type_repr = None;
        doc = None;
      }

  let meta_of_attrs = function
    | None -> default_meta
    | Some attrs ->
      List.fold_left
        (fun meta attr ->
          match attr with
          | `Multi | `List -> { meta with Elastic_t.list = `True; multi = true }
          | `ListSometimes -> { meta with Elastic_t.list = `Sometimes }
          | `Optional -> { meta with Elastic_t.optional = Some true }
          | `NotOptional -> { meta with Elastic_t.optional = Some false }
          | `Ignore -> { meta with Elastic_t.ignore = true }
          | `FieldsDefaultOptional -> { meta with Elastic_t.fields_default_optional = Some true }
          | `TypeOverride override -> { meta with Elastic_t.type_override = Some override }
          | `TypeRepr repr -> { meta with Elastic_t.type_repr = Some repr }
        )
        default_meta attrs

  let rec remap fields =
    let remap (name, (field : field)) =
      name, { field with fields = remap field.fields; properties = remap field.properties }
    in
    List.sort ~cmp:(String.compare $$ fst) fields |> List.map remap

  let mapping ?(dynamic = `True) ?source_excludes ?routing_required ?(attrs : attr list option) properties =
    let meta = meta_of_attrs attrs in
    let source =
      match source_excludes with
      | Some excludes -> Some { Elastic_t.excludes }
      | None -> None
    in
    let routing =
      match routing_required with
      | Some required -> Some { Elastic_t.required }
      | None -> None
    in
    { meta; mapping = { Elastic_t.dynamic; routing; source }; properties = remap properties }

  let field type_ ?analyzer ?dynamic ?eager_global_ordinals ?enabled ?fielddata ?(fields = []) ?format ?ignore_above
    ?index ?(properties = []) ?search_analyzer ?store ?term_vector ?(attrs : attr list option) ?doc ()
    =
    let field =
      Elastic_v.create_index_type_mapping_field_base ~type_ ?analyzer ?dynamic ?eager_global_ordinals ?enabled
        ?fielddata ?format ?ignore_above ?index ?search_analyzer ?store ?term_vector ()
    in
    let meta = meta_of_attrs attrs in
    let meta = { meta with doc } in
    { meta; field; fields; properties }

  let rec to_index_type_mapping_field (name, { meta = _; field; fields; properties }) =
    let ({
           type_;
           analyzer;
           dynamic;
           eager_global_ordinals;
           enabled;
           fielddata;
           format;
           ignore_above;
           index;
           search_analyzer;
           store;
           term_vector;
         }
          : Elastic_t.index_type_mapping_field_base
          )
      =
      field
    in
    ( name,
      Elastic_t.
        {
          type_;
          analyzer;
          dynamic;
          eager_global_ordinals;
          enabled;
          fielddata;
          format;
          ignore_above;
          index;
          search_analyzer;
          store;
          term_vector;
          fields = List.map to_index_type_mapping_field fields;
          properties = List.map to_index_type_mapping_field properties;
        } )

  let to_index_type_mapping { meta = _; mapping; properties } =
    let ({ dynamic; routing; source } : Elastic_t.index_type_mapping_base) = mapping in
    let properties = List.map to_index_type_mapping_field properties in
    { Elastic_t.dynamic; routing; source; properties }

  let rec to_index_type_mapping_field_annot (name, { meta; field; fields; properties }) =
    let ({
           type_;
           analyzer;
           dynamic;
           eager_global_ordinals;
           enabled;
           fielddata;
           format;
           ignore_above;
           index;
           search_analyzer;
           store;
           term_vector;
         }
          : Elastic_t.index_type_mapping_field_base
          )
      =
      field
    in
    let type_ = Option.default type_ meta.type_override in
    let meta = { meta with Elastic_t.type_override = None } in
    let meta = if meta <> default_meta then Some meta else None in
    ( name,
      Elastic_t.
        {
          meta;
          type_;
          analyzer;
          dynamic;
          eager_global_ordinals;
          enabled;
          fielddata;
          format;
          ignore_above;
          index;
          search_analyzer;
          store;
          term_vector;
          fields = List.map to_index_type_mapping_field_annot fields;
          properties = List.map to_index_type_mapping_field_annot properties;
        } )

  let to_index_type_mapping_annot { meta; mapping; properties } =
    let meta = if meta <> default_meta then Some meta else None in
    let ({ dynamic; routing; source } : Elastic_t.index_type_mapping_base) = mapping in
    let properties = List.map to_index_type_mapping_field_annot properties in
    { Elastic_t.meta; dynamic; routing; source; properties }

  let int ?doc ?attrs name = name, field `Long ?doc ?attrs ()

  let bool ?doc ?attrs name = name, field `Bool ?doc ?attrs ()

  let float_single ?doc ?attrs name = name, field `Float ?doc ?attrs ()

  let float ?doc ?attrs name = name, field `Double ?doc ?attrs ()

  let date ?doc ?attrs name = name, field `Date ?doc ?attrs ~format:"epoch_millis||date_optional_time" ()

  let strict_date ?doc ?attrs name = name, field `Date ?doc ?attrs ~format:"epoch_millis||strict_date_optional_time" ()

  (* Only here for retro-compatibility, for index created with previously broken format *)
  let old_broken_date ?doc ?attrs name = name, field `Date ?doc ?attrs ~format:"epoch_millis||dateOptionalTime" ()

  let ip ?doc ?attrs name = name, field `IP ?doc ?attrs ()

  let searchable' = field `Text ~term_vector:`WithPositionsOffsets

  let searchable ?doc ?attrs name = name, searchable' ?doc ?attrs ()

  let keyword' = field `Keyword

  let keyword ?doc ?attrs ?fields name = name, keyword' ?doc ?attrs ?fields ()

  let text ?doc ?attrs ?fields ?term_vector ?fielddata ~analyzer name =
    name, field `Text ?doc ?attrs ?fields ?term_vector ?fielddata ~analyzer ()

  let text2 ?doc ?attrs ?term_vector ?fielddata ~index_analyzer ~search_analyzer name =
    name, field `Text ?doc ?attrs ?term_vector ?fielddata ~analyzer:index_analyzer ~search_analyzer ()

  let murmur3 ?attrs name = name, field `Murmur3 ?attrs ()

  let object_ ?doc ?attrs ?dynamic name properties = name, field `Object ?doc ?attrs ?dynamic ~properties ()

  let nested ?doc ?attrs name properties = name, field `Nested ?doc ?attrs ~properties ()

  let disabled ?doc ?attrs name = name, field `Object ?doc ?attrs ~enabled:false ()

  let hashed hash (name, (t : field)) = name, { t with fields = hash "hash" :: t.fields }

  let stored (name, (t : field)) = name, { t with field = Elastic_t.{ t.field with store = true } }

  let not_indexed (name, (t : field)) = name, { t with field = Elastic_t.{ t.field with index = false } }

  let ignore_above n (name, (t : field)) = name, { t with field = Elastic_t.{ t.field with ignore_above = Some n } }

  let eager_global_ordinals eager_global_ordinals (name, (t : field)) =
    name, { t with field = Elastic_t.{ t.field with eager_global_ordinals } }

  let add_field field (name, (t : field)) = name, { t with fields = field :: t.fields }
end

let iter_bulk_result ~success ~fail s =
  let open Elastic_j in
  let result = bulk_result_of_string s in
  List.iter
    ( List.iter @@ fun (operation, { index; doc_type; id; status; error; _ }) ->
      Option.map_default fail success error index doc_type id operation status
    )
    result.items

let process_bulk_result ?(name = "") ?(check_conflict = false) ~t ?(stats = false) ?(ok_ctr = ref 0) ?(err_ctr = ref 0)
  ?(ovr_ctr = ref 0) ?f_success ?f_fail s
  =
  try
    let counts = CC.create () in
    let call f index doc_type id operation status =
      if log#level = `Debug then log#debug "%s/%s/%s %s status %d" index (default_kind doc_type) id operation status;
      match f with
      | None -> ()
      | Some f -> f index doc_type id operation
    in
    let success index doc_type id operation status =
      incr ok_ctr;
      CC.add counts operation;
      call f_success index doc_type id operation status
    in
    let fail { Elastic_j.error; reason } index doc_type id operation status =
      let cb =
        match status with
        | 404 ->
          CC.add counts (operation ^ "_missing");
          f_success
        | 409 when not check_conflict ->
          CC.add counts (operation ^ "_exists");
          f_success
        | _ ->
          incr err_ctr;
          if status = 429 then incr ovr_ctr;
          CC.add counts (operation ^ "_fail");
          log#warn "%s/%s/%s %s status %d [%s]: %s" index (default_kind doc_type) id operation status error
            (Option.default "null" reason);
          ( match f_fail with
          | None -> None
          | Some f -> Some (f error status)
          )
      in
      call cb index doc_type id operation status
    in
    iter_bulk_result ~success ~fail s;
    if stats then log#info "bulk_result %s elapsed %s stats %s" name t#get_str (CC.show counts id)
  with exn -> log#error ~exn "process_bulk_result %s: %s" name s

(** Scrolling. *)

(** Type of the close function, used in {!scroll_cb}. Indicates ES to
    stop scrolling. *)
type close = ?retry:int -> string option -> unit Lwt.t

(** Type of the scroll function, used in {!scroll_cb}. Indicates ES to
    continue scrolling. *)
type scroll = ?timeout:int -> ?count:int -> string option -> unit Lwt.t

(** Type of a scroll callback. The last argument is a string
    containing the search result: a json record that contains an
    optional _scroll_id field. Pass this field (as a string option) to
    either [scroll] or [close] to resp. continue or stop scrolling. *)
type scroll_cb = scroll:scroll -> close:close -> string -> unit Lwt.t

(** [scroll_lwt ~host ~index ?kind ~query cb] performs a scroll on
    [host]. Scrolling performs a search specified in [query] on
    [index]:[kind], and returns results via [cb]. See {!scroll_cb} doc
    for more info.
    NB: in general _do not_ sort while scrolling *)
let scroll_lwt ~hosts ?request_timeout ~index ?kind ~query ~timeout ?verbose ?setup ?limit ?(scan = true) ?size
  ?(args = []) f
  =
  let module Hosts = (val hosts : Hosts) in
  let limit =
    match limit with
    | None -> fun _count -> true
    | Some limit ->
      let total = ref 0 in
      fun count ->
        total += count;
        !total < limit
  in
  let log_es_error host msg code body =
    let error, reason =
      match Elastic_j.scroll_hits_of_string Elastic_j.read_safe_json body with
      | { Elastic_j.error = Some { error; reason }; _ } -> error, reason
      | _ -> "", Some body
      | exception _ -> "", Some body
    in
    log#error "scroll on %s : %s http %d [%s]: %s" host msg code error (Option.default "" reason)
  in
  let rec close state host ?(retry = 5) scroll_id =
    match scroll_id with
    | None -> Lwt.return_unit
    | Some scroll_id ->
      let url = sprintf "http://%s/_search/scroll" host in
      let%lwt ok =
        let body = `Raw ("application/json", Elastic_j.(string_of_clear_scroll { scroll_id = [ scroll_id ] })) in
        match%lwt
          Web.http_request_lwt' ~ua:(Pid.show_self ()) ?timeout:request_timeout ?verbose ?setup ~body `DELETE url
        with
        | `Ok (200, _) -> Lwt.return true
        | `Ok ((404 as code), s) ->
          log_es_error host "close context gone" code s;
          Lwt.return true
        | `Ok (code, s) ->
          log_es_error host "close" code s;
          Lwt.return false
        | `Error code ->
          log#error "scroll on %s : close curl (%d) %s" host (Curl.errno code) (Curl.strerror code);
          Lwt.return false
      in
      ( match ok with
      | true -> Lwt.return_unit
      | false when retry = 0 ->
        log#error "scroll close retry failed";
        Lwt.return_unit
      | _ ->
        let delay = Time.seconds 5 in
        log#warn "scroll close, will retry %d times after %s" retry (Time.duration_str delay);
        let%lwt () = Lwt_unix.sleep delay in
        close state (Hosts.random_host state) ~retry:(retry - 1) (Some scroll_id)
      )
  in
  let scroll_args =
    (* rest_total_hits_as_int should be passed on every call if it was provided initially *)
    match
      List.find_map_exn
        (function
          | (k, _) as args when k = rest_total_hits_as_int -> Some args
          | _ -> None
          )
        args
    with
    | args -> "?" ^ Web.make_url_args [ args ]
    | exception Not_found -> ""
  in
  let rec scroll state host ?(timeout = timeout) ?count id =
    match Daemon.should_exit () with
    | true ->
      log#info "scroll_lwt should exit";
      close state host id
    | _ ->
    match count with
    | Some count when not (limit count) ->
      log#info "limit reached";
      close state host id
    | _ ->
    match id with
    | None ->
      log#info "scroll_lwt done";
      Lwt.return_unit
    | Some scroll_id ->
      let url = sprintf "http://%s/_search/scroll%s" host scroll_args in
      let body =
        `Raw ("application/json", Elastic_j.(string_of_continue_scroll { scroll = sprintf "%ds" timeout; scroll_id }))
      in
      ( match%lwt
          Web.http_request_lwt' ~ua:(Pid.show_self ()) ?timeout:request_timeout ?verbose ?setup ~body `POST url
        with
      | exception exn ->
        log#error ~exn "scroll_lwt";
        let%lwt () = close state host (Some scroll_id) in
        Lwt.fail exn
      | `Ok (200, s) -> f ~scroll:(scroll state host) ~close:(close state host) s
      | `Ok ((404 as code), s) ->
        log_es_error "scroll timed out" host code s;
        Exn_lwt.fail "scroll timed out"
      | `Ok (code, s) ->
        log_es_error host "scroll" code s;
        let%lwt () = close state (if code / 100 = 5 then Hosts.random_host state else host) (Some scroll_id) in
        Exn_lwt.fail "http %d" code
      | `Error code ->
        log#error "scroll on %s: scroll curl error (%d) %s" host (Curl.errno code) (Curl.strerror code);
        let%lwt () = close state host (Some scroll_id) in
        Exn_lwt.fail "(%d) %s" (Curl.errno code) (Curl.strerror code)
      )
  in
  let args =
    List.filter_map id
      [
        (if scan then Some ("sort", "_doc") else None);
        ( match size with
        | Some size -> Some ("size", string_of_int size)
        | None -> None
        );
        Some ("scroll", string_of_int timeout ^ "s");
      ]
    @ args
  in
  let state = Hosts.new_request () in
  let host = Hosts.random_host state in
  let url =
    sprintf "http://%s/_search?%s"
      (String.concat "/" (List.filter_map id [ Some host; Some index; kind ]))
      (Web.make_url_args args)
  in
  match%lwt
    Web.http_query_lwt ~ua:(Pid.show_self ()) ?timeout:request_timeout ?verbose ?setup ~body:("application/json", query)
      `POST url
  with
  | `Ok s -> f ~scroll:(scroll state host) ~close:(close state host) s
  | `Error s -> Exn_lwt.fail "scroll_lwt %s %s" url s

module Hashed_action = struct
  type t = {
    index : string option;
    doc_type : string;
    operation : string;
    id : string option;
  }

  (* index will always be specified for the action build from ES response *)
  let equal a b =
    a.doc_type = b.doc_type
    && a.operation = b.operation
    && a.id = b.id
    && (a.index = b.index || a.index = None || b.index = None)

  let hash x = Farmhash.hash x.doc_type lxor Farmhash.hash x.operation lxor Farmhash.hash (Option.default "" x.id)

  let show x = sprintf "%s /%s/%s/%s" x.operation (Option.default "" x.index) x.doc_type (Option.default "" x.id)

  let make ~action_key_mode index doc_type ?id operation =
    {
      index = (if action_key_mode = Elastic_intf.Full then Some index else None);
      doc_type = default_kind doc_type;
      id;
      operation;
    }
end

module ActionHashtbl = Hashtbl.Make (Hashed_action)

let operation_of_action = function
  | `Delete -> "delete"
  | `Index _ -> "index"
  | `Create _ -> "create"
  | `Update _ -> "update"

let doc_of_action = function
  | `Index doc | `Create doc | `Update doc -> Some (Lazy.force doc)
  | `Delete -> None

let format_command { Elastic_intf.action; meta; tag = _ } =
  Elastic_j.string_of_index_command [ operation_of_action action, meta ]
  ^ "\n"
  ^
  match doc_of_action action with
  | Some doc -> doc ^ "\n"
  | None -> ""

let make_retry_enum ?(action_key_mode = Elastic_intf.Full) ?f_success ?f_fail ?f_fail_all ?f_retry ?f_fail_retry () =
  let htbl = ActionHashtbl.create 1 in
  (* documents with IDs *)
  let htbl_auto = ActionHashtbl.create 1 in
  (* documents without IDs *)
  let retry_queue = Queue.create () in
  let htbl_keys h = ActionHashtbl.fold (fun k _ acc -> Hashed_action.show k :: acc) h [] |> List.rev in
  let make_key = Hashed_action.make ~action_key_mode in
  let start (retry, ({ Elastic_intf.action; meta = { Elastic_j.index; doc_type; id; _ }; _ } as e)) =
    let operation = operation_of_action action in
    match id with
    | Some id ->
      let k = make_key index doc_type ~id operation in
      (* NOTE
         If you encounter the following warning, it means you are trying to act on the same document more than once in a short period of time.
         Since there is no way to distinguish between the results for two actions on the same doc id, only one of the actions can be retried.
      *)
      if ActionHashtbl.mem htbl k then
        log#warn "retry_enum duplicate action %S, retry may not work properly" (Hashed_action.show k);
      ActionHashtbl.add htbl k
        ( ( match retry with
          | Some n -> Some (n - 1)
          | None -> None
          ),
          e
        )
    | None ->
      let k = make_key index doc_type operation in
      let l =
        try ActionHashtbl.find htbl_auto k
        with Not_found ->
          let v = ref [] in
          ActionHashtbl.add htbl_auto k v;
          v
      in
      tuck l
        ( ( match retry with
          | Some n -> Some (n - 1)
          | None -> None
          ),
          e
        )
  in
  let next () =
    match Queue.pop retry_queue with
    | e -> e
    | exception Queue.Empty -> raise Enum.No_more_elements
  in
  let retry = function
    | Some n, v when n < 0 -> call_me_maybe f_fail_retry v
    | (_, v) as e ->
      Queue.push e retry_queue;
      call_me_maybe f_retry v
  in
  let success index doc_type id operation =
    let k = make_key index doc_type ~id operation in
    match ActionHashtbl.find htbl k with
    | _, v ->
      call_me_maybe f_success v;
      ActionHashtbl.remove htbl k
    | exception Not_found ->
      let k' = make_key index doc_type operation in
      (*
        NOTE when using autogenerated document IDs (id=None), ES will replace `Index with `Create,
        hence this function will not find the corresponding key and will complain.
      *)
      ( match ActionHashtbl.mem htbl_auto k' with
      | true -> ()
      | false ->
        log#warn "retry_enum success callback was not called because key does not exist: %s" (Hashed_action.show k);
        if log#level = `Debug then (
          log#debug "htbl %s" (Stre.list Prelude.id (htbl_keys htbl));
          log#debug "htbl_auto %s" (Stre.list Prelude.id (htbl_keys htbl_auto))
        )
      )
  in
  let done_all () =
    ( match f_success with
    | Some f -> ActionHashtbl.iter (fun _ l -> List.iter (fun (_, v) -> f v) !l) htbl_auto
    | None -> ()
    );
    ActionHashtbl.clear htbl_auto
  in
  let fail error status index doc_type id operation =
    let k = make_key index doc_type ~id operation in
    match ActionHashtbl.find htbl k with
    | exception Not_found ->
      let k = make_key index doc_type operation in
      ( match ActionHashtbl.find htbl_auto k with
      | exception Not_found ->
        log#warn "retry_enum fail callback was not called because key does not exist: %s" (Hashed_action.show k)
      | l ->
        log#warn "retry_enum document with auto id, retrying the whole batch of %d elements" (List.length !l);
        List.iter retry !l;
        l := []
      )
    | e ->
      ActionHashtbl.remove htbl k;
      ( match f_fail with
      | Some f when not (f e error status) -> log#warn "retry_enum retry cancelled for %s" (Hashed_action.show k)
      | _ -> retry e
      )
  in
  let fail_all error status =
    ( match f_fail_all with
    | Some f when not (f error status) ->
      let keys = List.append (htbl_keys htbl) (htbl_keys htbl_auto) in
      log#warn "retry_enum retry cancelled for %s" (Stre.list id keys)
    | _ ->
      ActionHashtbl.iter (fun _ -> retry) htbl;
      ActionHashtbl.iter (fun _ l -> List.iter retry !l) htbl_auto
    );
    ActionHashtbl.clear htbl;
    ActionHashtbl.clear htbl_auto
  in
  Enum.from next, start, success, done_all, fail, fail_all

let make_version version =
  match version with
  | `Auto -> None, None
  | `Internal v -> None, Some v
  | `External_gte v -> Some "external_gte", Some v
  | `External_gt v -> Some "external_gt", Some v

let make_lazy_command ~tag ?routing ?parent ?(version = `Auto) ~index ~doc_type action id =
  let version_type, version = make_version version in
  { action; meta = { Elastic_j.index; doc_type; id; routing; version; version_type; parent }; tag }

let lazyfy = function
  | `Index doc -> `Index (Lazy.from_val doc)
  | `Create doc -> `Create (Lazy.from_val doc)
  | `Update doc -> `Update (Lazy.from_val doc)
  | `Delete -> `Delete

let make_command ~tag ?routing ?parent ?version ~index ~doc_type action id =
  make_lazy_command ~tag ?routing ?parent ?version ~index ~doc_type (lazyfy action) id

let unpack_command ({ index; doc_type; id; routing; version; action } : Elastic_intf.command) =
  make_command ~tag:() ?routing ?version ~index ~doc_type action id

let wrap_upsert ?(upsert = true) doc = `Assoc [ "doc", doc; "doc_as_upsert", `Bool upsert ]

module Query (Http : Web.HTTP) (H : Hosts) : Elastic_intf.Query with type 'a t = 'a Http.IO.t = struct
  include H
  module T = Http.IO
  module J = Yojson.Basic

  type 'a t = 'a T.t

  let ( >>= ) = T.( >>= )

  exception ESError of int * string

  let es_failwith code = ksprintf (fun msg -> T.raise @@ ESError (code, msg))

  let should_exit () = if not @@ Daemon.should_run () then T.raise Daemon.ShouldExit else T.return ()

  let es_fail name host code s = es_failwith code "%s on %s : HTTP (%d) %s" name host code s

  let loop_request ?pdebug ?(version_conflict_handler = es_fail) ?(once = false) ~name ?(should_exit = should_exit)
    ?timeout ?retries ?(retry_init = Time.seconds 5) ?(retry_factor = 1.3) ?(retry_max = Time.seconds 60) req
    =
    let timer = new Action.timer in
    let state = H.new_request () in
    let rec loop ~wait_retry host attempt =
      call_me_maybe pdebug (sprintf "# ATTEMPT #%d\n" attempt);
      let retry host code error_msg =
        let elapsed = int_of_float timer#get in
        match retries, timeout, once with
        | _, _, true -> es_failwith code "%s on %s : %s (elapsed %ds)" name host error_msg elapsed
        | _, Some t, _ when elapsed >= t ->
          es_failwith code "%s on %s : %s : time limit reached (elapsed %ds >= %ds, tries %d)" name host error_msg
            elapsed t attempt
        | Some n, _, _ when attempt >= n ->
          es_failwith code "%s on %s : %s : retry limit reached (elapsed %ds, tries %d >= %d)" name host error_msg
            elapsed attempt n
        | _ ->
          let pause = wait_retry +. Random.float retry_init in
          log#warn "%s on %s : %s, will retry in %s (elapsed %ds, %d attempts)" name host error_msg
            (Time.duration_str pause) elapsed attempt;
          should_exit () >>= fun () ->
          T.sleep pause >>= fun () ->
          loop ~wait_retry:(min retry_max (wait_retry *. retry_factor)) (random_host state) (succ attempt)
      in
      req host >>= function
      | `Ok (404, _) -> T.return None
      | `Ok (code, s) when code / 100 = 2 -> T.return @@ Some s
      | `Ok (409, s) -> version_conflict_handler name host 409 s
      | `Ok (code, s) when code / 100 = 4 -> es_failwith code "%s on %s : HTTP (%d) %s" name host code s
      | `Ok (code, s) -> retry host code (sprintf "HTTP (%d) %s" code s)
      | `Error code -> retry host (-1) (sprintf "CURL (%d) %s" (Curl.errno code) (Curl.strerror code))
    in
    let host = H.random_host state in
    loop ~wait_retry:retry_init host 1

  let string_of_kinds = function
    | [] -> ""
    | l ->
      let str = l |> List.map Web.urlencode |> String.concat "," in
      sprintf "%s/" str

  let request ?pdebug ?version_conflict_handler ~name ?should_exit ?verbose ?once ?timeout ?setup ?retries ?body
    ?(headers = []) action path ?(args = []) ()
    =
    let path = List.map Web.urlencode path in
    let make_url host =
      let is_args, args =
        match args with
        | [] -> "", ""
        | args -> "?", Web.make_url_args args
      in
      sprintf "http://%s%s%s" (String.concat "/" (host :: path)) is_args args
    in
    let body =
      match body with
      | None -> None
      | Some body -> Some (`Raw ("application/json", body))
    in
    let headers = List.map (fun (k, v) -> sprintf "%s: %s" k v) headers in
    let pdebug_curl url =
      match pdebug with
      | None -> ()
      | Some pdebug ->
        let quote = Filename.quote in
        let b = Buffer.create 10 in
        bprintf b "curl";
        if action <> `GET then bprintf b " -X%s" (Web.string_of_http_action action);
        headers |> List.iter (fun s -> bprintf b " -H %s" (quote s));
        bprintf b " %s" (quote url);
        begin
          match body with
          | None -> ()
          | Some (`Raw (ctype, body)) ->
            bprintf b " -H %s" (quote @@ sprintf "Content-Type: %s" ctype);
            bprintf b " -d %s" (quote body)
        end;
        pdebug @@ Buffer.contents b
    in
    let do_request host =
      let url = make_url host in
      pdebug_curl url;
      Http.http_request' ~ua:(Pid.show_self ()) ~headers ?timeout ?verbose ?setup ?body action url
    in
    loop_request ?pdebug ?version_conflict_handler ~name ?should_exit ?timeout ?retries ?once do_request

  (** [get_id ~host ~index ~kind id] returns the document of kind [kind] and id [id] from index [index].
      @return Document identified by [id] as a string *)
  let get_id ~index ~kind ?should_exit ?verbose ?once ?timeout ?setup ?retries ?args id =
    request ~name:"get_id" ?should_exit ?timeout ?retries ?once ?verbose ?setup `GET [ index; kind; id ] ?args ()

  let get_id_kinds ~index ?(kinds = []) ?should_exit ?verbose ?once ?timeout ?setup ?retries ?args id =
    get_id ~index ~kind:(string_of_kinds kinds) ?should_exit ?verbose ?once ?timeout ?setup ?retries ?args id

  let multiget ?index ?kind ?should_exit ?verbose ?once ?timeout ?setup ?retries ?args docs =
    let docs = Elastic_j.string_of_multiget { Elastic_j.docs } in
    let path = List.filter_map id [ index; kind; Some "_mget" ] in
    request ~name:"multiget" ?should_exit ?timeout ?retries ?once ?verbose ?setup ~body:docs `GET path ?args ()

  let multiget_ids ~index ?kind ?should_exit ?verbose ?once ?timeout ?setup ?retries ?args ids =
    let ids = Yojson.Safe.to_string (`Assoc [ "ids", `List (List.map (fun x -> `String x) ids) ]) in
    let path = List.filter_map id [ Some index; kind; Some "_mget" ] in
    request ~name:"multiget_ids" ?should_exit ?timeout ?retries ?once ?verbose ?setup ~body:ids `GET path ?args ()

  (** [search ~index ~kind ~args query] performs a search with query
      [query] (a JSON ES query) on the ES server specified in {!host},
      on index [index] and kind [kind]. Using {!scroll_lwt} puts less
      strain of ES servers when retrieving a huge amount of data. *)
  let search ?pdebug ~index ?kind ?verbose ?should_exit ?once ?timeout ?setup ?retries ?args query =
    let path = List.filter_map id [ Some index; kind; Some "_search" ] in
    let body = Elastic_query_dsl.top_query_to_string query in
    request ?pdebug ~name:"search" ?should_exit ?timeout ?retries ?once ?verbose ?setup ~body `POST path ?args ()

  let search_kinds ~index ?(kinds = []) ?verbose ?should_exit ?once ?timeout ?setup ?retries ?args query =
    search ~index ~kind:(string_of_kinds kinds) ?verbose ?should_exit ?once ?timeout ?setup ?retries ?args query

  let search_shards ~index ?verbose ?should_exit ?once ?timeout ?setup ?retries ?args () =
    request ~name:"search_shards" ?verbose ?should_exit ?once ?timeout ?setup ?retries `GET [ index; "_search_shards" ]
      ?args ()
    >>= function
    | None -> T.return None
    | Some s ->
    match Elastic_j.search_shards_of_string s with
    | result -> T.return (Some result)
    | exception exn -> T.raise exn

  let get_index_settings ~index ?verbose ?should_exit ?once ?timeout ?setup ?retries ?args () =
    request ~name:"get_index_settings" ?verbose ?should_exit ?once ?timeout ?setup ?retries `GET [ index; "_settings" ]
      ?args ()
    >>= function
    | None -> T.return None
    | Some s ->
    match Elastic_j.index_settings_result_of_string s with
    | result -> T.return (Some result)
    | exception exn -> T.raise exn

  let wait_pause pause =
    log#info "will wait for %s" (Time.duration_str pause);
    let need_stamp = Time.now () +. pause in
    let rec wait () = if Time.now () < need_stamp && Daemon.should_run () then T.sleep 5. >>= wait else T.return () in
    wait

  (** when [id] not specified, elastic will generate random, but [args] and [timeout] will be ignored *)
  let put_string ?should_exit ?version_conflict_handler ~index ~kind ?id ?once ?timeout ?setup ?retries ?(args = [])
    body
    =
    let args =
      match timeout with
      | Some t -> ("timeout", sprintf "%ds" t) :: args
      | _ -> args
    in
    let action, args =
      match id with
      | Some _ -> `PUT, Some args
      | None -> `POST, None
    in
    let path =
      index
      :: kind
      ::
      ( match id with
      | Some id -> [ id ]
      | None -> []
      )
    in
    request ?version_conflict_handler ~name:"put" ?once ?retries ?should_exit ?timeout ?setup ~body action path ?args ()

  let put ?should_exit ~index ~kind ?id ?once ?timeout ?setup ?retries ?args body =
    put_string ?should_exit ~index ~kind ?id ?once ?timeout ?setup ?retries ?args (J.to_string body) >>= function
    | None -> T.return None
    | Some s ->
    match Elastic_j.index_result_of_string s with
    | result -> T.return (Some result)
    | exception exn -> T.raise exn

  let make_version_args = function
    | Version version -> [ "version", string_of_int version ]
    | Seq_no_primary_term { seq_no; primary_term } ->
      [ "if_seq_no", string_of_int seq_no; "if_primary_term", string_of_int primary_term ]

  let put_versioned ?should_exit ~index ~kind ~id ~version ?once ?timeout ?setup ?retries ?(args = []) body =
    let args = make_version_args version @ args in
    let body = J.to_string body in
    let version_conflict_handler _name _host _code _s = T.return None in
    put_string ~version_conflict_handler ?should_exit ~index ~kind ~id ?once ?timeout ?setup ?retries ~args body
    >>= function
    | Some _ -> T.return true
    | None -> T.return false

  let delete ?should_exit ~index ~kind ~id ?once ?timeout ?setup ?retries ?(args = []) () =
    let args =
      match timeout with
      | Some t -> ("timeout", sprintf "%ds" t) :: args
      | _ -> args
    in
    let path = [ index; kind; id ] in
    request ?timeout ?setup ~name:"delete" ?once ?retries ?should_exit `DELETE path ~args () >>= fun _ -> T.return ()

  let update_string ?should_exit ~index ~kind ~id ?once ?timeout ?setup ?retries ?(args = []) body =
    let args =
      match timeout with
      | Some t -> ("timeout", sprintf "%ds" t) :: args
      | _ -> args
    in
    let path = [ index; kind; id; "_update" ] in
    request ~name:"update" ?once ?retries ?should_exit ?timeout ?setup ~body `POST path ~args ()

  let update ?should_exit ?upsert ~index ~kind ~id ?once ?timeout ?setup ?retries ?args body =
    let body = wrap_upsert ?upsert body |> J.to_string in
    update_string ?should_exit ~index ~kind ~id ?once ?timeout ?setup ?retries ?args body >>= fun _ -> T.return ()

  let update_versioned ?should_exit ?upsert ~index ~kind ~id ~version ?once ?timeout ?setup ?retries ?(args = []) body =
    let body = wrap_upsert ?upsert body |> J.to_string in
    let args = make_version_args version @ args in
    let args =
      match timeout with
      | Some t -> ("timeout", sprintf "%ds" t) :: args
      | _ -> args
    in
    let version_conflict_handler _name _host _code _s = T.return None in
    let path = [ index; kind; id; "_update" ] in
    request ~version_conflict_handler ~name:"update" ?once ?retries ?should_exit ?timeout ?setup ~body `POST path ~args
      ()
    >>= function
    | Some _ -> T.return true
    | None -> T.return false

  let refresh ?retries ?timeout ?setup indices =
    match indices with
    | [] ->
      log#warn "no indices to refresh";
      T.return ()
    | indices ->
      let path = [ String.concat "," indices; "_refresh" ] in
      request ~name:"refresh" ?retries ~verbose:true ?setup ?timeout `POST path () >>= fun s ->
      log#debug "refresh %s" (Option.default "Not found" s);
      T.return ()

  let aliases ?retries ?timeout ?setup ?index () =
    match index with
    | None -> request ~name:"aliases" ?retries ?timeout ~verbose:true ?setup `GET [ "_aliases" ] ()
    | Some index -> request ~name:"alias" ?retries ?timeout ~verbose:true ?setup `GET [ index; "_alias" ] ()

  let docs_stats ?retries ?timeout ?setup index =
    request ~name:"docs_stats" ?retries ?timeout ~verbose:true ?setup `GET [ index; "_stats"; "docs" ] ()

  (* FIXME limit = Limits.elasticsearch_http_limit*)
  let bulk_write_worker ?(dry_run = false) ?(name = "") ~hosts ~f_success ~f_done_all ~f_fail ~f_fail_all
    ?connecttimeout ?(timeout = 60) ?setup ?(chunked = false) ?check_conflict ?(limit_bytes = 10_000_000) ?(args = [])
    ~make ~mark enum
    =
    match Enum.is_empty enum with
    | true -> T.return ()
    | false ->
      let make (_retry, e) = make e in
      ( match dry_run with
      | true ->
        Enum.iter
          (fun ((_, { action; meta = { Elastic_j.index; doc_type; id; _ }; _ }) as e) ->
            let (_ : string) = make e in
            let () = mark e in
            let () = f_success index doc_type (Option.default "<auto>" id) (operation_of_action action) in
            ()
          )
          enum;
        T.return ()
      | false ->
        let content_type = "application/x-ndjson" in
        let content_type_header = "Content-Type: " ^ content_type in
        let setup = call_me_maybe setup in
        let setup, body =
          match chunked with
          | true ->
            let open Curl in
            let read = Io_utils.chunked_read_lax enum ~limit_bytes ~mark make in
            let read i =
              let s, o, l = read i in
              String.sub s o l
            in
            let setup h =
              Option.may (set_connecttimeoutms h $ Time.to_ms) connecttimeout;
              set_readfunction h read;
              set_httpheader h [ content_type_header; "Transfer-Encoding: chunked" ];
              setup h
            in
            setup, None
          | false ->
            let contents = Io_utils.read_up_to enum ~limit_bytes ~mark make in
            let setup h =
              Option.may (Curl.set_connecttimeoutms h $ Time.to_ms) connecttimeout;
              Curl.set_httpheader h [ content_type_header ];
              setup h
            in
            setup, Some (`Raw (content_type, contents))
        in
        let host = random_host hosts in
        let url =
          sprintf "http://%s/_bulk?%s" host (Web.make_url_args (("timeout", string_of_int timeout ^ "s") :: args))
        in
        let t = new Action.timer in
        Http.http_request' ~ua:(Pid.show_self ()) ~verbose:true ~timeout:(timeout * 5) ~setup ?body `POST url
        >>= ( function
        | `Ok (200, s) ->
          process_bulk_result ~name ?check_conflict ~t ~stats:true ~f_success ~f_fail s;
          f_done_all ();
          T.return ()
        | `Ok (code, error) ->
          log#error "bulk_write %s to %s : http %d %s" name url code
            (if log#level = `Debug || code = 400 then error else Stre.shorten 256 error);
          f_fail_all error code;
          T.return ()
        | `Error code ->
          let error = Curl.strerror code in
          let code = Curl.errno code in
          log#error "bulk_write %s to %s : (%d) %s" name url code error;
          f_fail_all error code;
          wait_pause 10. () )
      )

  let bulk_write_wrapper ?dry_run ?(name = "") ?chunked ?limit_bytes ?stats:stats' ?f_success ?f_fail ?f_fail_all
    ?f_retry ?f_fail_retry ?action_key_mode ?connecttimeout ?timeout ?setup ?args ?check_conflict cb
    =
    if log#level = `Debug then log#debug "bulk_write %s" name;
    let verbose = log#level = `Debug in
    let log_s = if verbose then log#debug "bulk_write %s : %s" name $ String.rchop else (ignore : string -> unit) in
    let stats =
      match stats' with
      | None -> CC.create ()
      | Some stats -> stats
    in
    let t = new Action.timer in
    let show_stats () =
      log#info "bulk_write %s elapsed %s stats %s" name t#get_str (CC.show stats id);
      t#reset
    in
    let tick = Action.period 1_000_000 (fun _ -> show_stats ()) in
    let show_stats = if stats' = None then show_stats else fun () -> () in
    let f_success v =
      CC.add stats "success";
      call_me_maybe f_success v
    in
    let f_fail elem error status =
      CC.add stats "fail";
      match f_fail with
      | Some f -> f elem error status
      | None -> true
    in
    let f_fail_all error code =
      CC.add stats "fail_all";
      match f_fail_all with
      | Some f -> f error code
      | None -> true
    in
    let f_retry v =
      CC.add stats "retry";
      call_me_maybe f_retry v
    in
    let f_fail_retry v =
      CC.add stats "fail_retry";
      call_me_maybe f_fail_retry v
    in
    let retry, mark, f_success, f_done_all, f_fail, f_fail_all =
      make_retry_enum ?action_key_mode ~f_success ~f_fail ~f_fail_all ~f_retry ~f_fail_retry ()
    in
    let mark (retry, e) =
      CC.add stats "mark";
      mark (retry, e)
    in
    let make cmd =
      CC.add stats "output";
      tick ();
      let cmd = format_command cmd in
      log_s cmd;
      cmd
    in
    let hosts = H.new_request () in
    let bulk_write ?f_fail:f_fail' ?f_fail_all:f_fail_all' enum =
      let f_fail =
        match f_fail' with
        | None -> f_fail
        | Some f ->
          fun error status index doc_type id operation ->
            f error status;
            f_fail error status index doc_type id operation
      in
      let f_fail_all =
        match f_fail_all' with
        | None -> f_fail_all
        | Some f ->
          fun error code ->
            f error code;
            f_fail_all error code
      in
      bulk_write_worker ?dry_run ~name ~hosts ~f_success ~f_done_all ~f_fail ~f_fail_all ?connecttimeout ?timeout ?setup
        ?chunked ?limit_bytes ?args ?check_conflict ~make ~mark enum
    in
    cb retry bulk_write >>= fun () ->
    show_stats ();
    T.return ()

  let retry_loop' ?f_fail ?f_fail_all ?(preflight = T.return) enum retry bulk_write =
    (*
      Pass the latest enum to the loop function on every iteration, since it may have been peeked into and
      may have a cached element waiting to be extracted, while the original enum will not have that element.
    *)
    let rec loop enum () =
      match Enum.append retry enum with
      | enum when Enum.is_empty enum -> T.return ()
      | enum -> preflight () >>= (fun () -> bulk_write ?f_fail ?f_fail_all enum) >>= loop enum
    in
    loop enum ()

  let retry_loop enum retry bulk_write = retry_loop' enum retry bulk_write

  let retry_with_backoff' ?(init_delay = Time.seconds 5) ?(multiplier = 1.3) ?(variation = 0.5) enum retry bulk_write =
    let overload = ref 0 in
    let delay = ref init_delay in
    let f_fail _error status = if status = 429 then incr overload in
    let f_fail_all _error _code = incr overload in
    let preflight () =
      match !overload > 0 with
      | true ->
        let var_delay = !delay *. (1. +. ((1. -. multiplier) *. (1. -. Random.float variation))) in
        log#info "will wait for %s because of overload (%d)" (Time.duration_str var_delay) !overload;
        delay := !delay *. multiplier;
        overload := 0;
        wait_pause var_delay ()
      | false ->
        delay := init_delay;
        overload := 0;
        T.return ()
    in
    retry_loop' ~f_fail ~f_fail_all ~preflight enum retry bulk_write

  let retry_with_backoff enum retry bulk_write = retry_with_backoff' enum retry bulk_write

  type ('a, 'b) bulk_write_common =
    ?dry_run:bool ->
    ?name:string ->
    ?chunked:bool ->
    ?limit_bytes:int ->
    ?stats:string Cache.Count.t ->
    ?f_success:('a bulk_action -> unit) ->
    ?f_fail:(int option * 'a bulk_action -> string -> int -> bool) ->
    ?f_fail_all:(string -> int -> bool) ->
    ?f_retry:('a bulk_action -> unit) ->
    ?f_fail_retry:('a bulk_action -> unit) ->
    ?action_key_mode:action_key_mode ->
    ?connecttimeout:Time.t ->
    ?timeout:int ->
    ?setup:(Curl.t -> unit) ->
    ?args:(string * string) list ->
    ?retry:int ->
    ?check_conflict:bool ->
    ?loop:('a, unit t) bulk_write_loop ->
    'b Enum.t ->
    unit t

  let bulk_write' ?dry_run ?name ?chunked ?limit_bytes ?stats ?f_success ?f_fail ?f_fail_all ?f_retry ?f_fail_retry
    ?action_key_mode ?connecttimeout ?timeout ?setup ?args ?retry ?check_conflict ?(loop = retry_loop) enum
    =
    let enum = Enum.map (fun e -> retry, e) enum in
    bulk_write_wrapper ?dry_run ?name ?chunked ?limit_bytes ?stats ?f_success ?f_fail ?f_fail_all ?f_retry ?f_fail_retry
      ?action_key_mode ?connecttimeout ?timeout ?setup ?check_conflict ?args (loop enum)

  let bulk_write ?dry_run ?name ?chunked ?limit_bytes ?stats ?f_success ?f_fail ?f_fail_all ?f_retry ?f_fail_retry
    ?action_key_mode ?connecttimeout ?timeout ?setup ?args ?retry ?check_conflict ?loop enum
    =
    let enum = Enum.map unpack_command enum in
    bulk_write' ?dry_run ?name ?chunked ?limit_bytes ?stats ?f_success ?f_fail ?f_fail_all ?f_retry ?f_fail_retry
      ?action_key_mode ?connecttimeout ?timeout ?setup ?args ?retry ?check_conflict ?loop enum
end

(* Query *)

module type Query_lwt_t = Elastic_intf.Query with type 'a t = 'a Lwt.t

module type Query_blocking_t = Elastic_intf.Query with type 'a t = 'a

let bulk_write_stream ~elastic ?setup ?dry_run ?name ?limit_bytes ?stats ?f_success ?f_fail ?f_fail_all ?f_retry
  ?f_fail_retry ?args ?action_key_mode ?retry ?loop:bulk_write_loop stream
  =
  let module Elastic = (val elastic : Query_lwt_t) in
  let rec continue enum =
    match Enum.is_empty enum with
    | false ->
      let%lwt () =
        Elastic.bulk_write' ?setup ?dry_run ?name ?limit_bytes ?stats ?f_success ?f_fail ?f_fail_all ?f_retry
          ?f_fail_retry ?args ?action_key_mode ?retry ?loop:bulk_write_loop enum
      in
      continue enum
    | true ->
      ( match%lwt Lwt_stream.peek stream with
      | None -> Lwt.return_unit
      | Some _ -> get_more ()
      )
  and get_more () =
    let enum =
      Enum.from (fun () ->
        match Lwt_stream.get_available_up_to 1 stream with
        | [] -> raise Enum.No_more_elements
        | [ x ] -> x
        | _ -> assert false
      )
    in
    continue enum
  in
  get_more ()

let utf16le_of_utf8 s =
  let b = Buffer.create (String.length s * 2) in
  let d = Uutf.decoder ~encoding:`UTF_8 (`String s) in
  let e = Uutf.encoder `UTF_16LE (`Buffer b) in
  let rec loop d e =
    match Uutf.decode d with
    | `Uchar _ as u ->
      ignore (Uutf.encode e u);
      loop d e
    | `End -> ignore (Uutf.encode e `End)
    | `Malformed _ -> Exn.fail "malformed utf8"
    | `Await -> assert false
  in
  let () = loop d e in
  Buffer.contents b

let murmur3_hash s = Murmur3.hash32 (utf16le_of_utf8 s)

let _murmur3_test =
  let open OUnit in
  "murmur3_hash" >:: fun () ->
  let t s t =
    let h = murmur3_hash s in
    OUnit.assert_equal ~printer:Int32.to_string ~msg:s h t
  in
  t "hell" 0x5a0cb7c3l;
  t "hello" 0xd7c31989l;
  t "hello w" 0x22ab2984l;
  t "hello wo" 0xdf0ca123l;
  t "hello wor" 0xe7744d61l;
  t "The quick brown fox jumps over the lazy dog" 0xe07db09cl;
  t "The quick brown fox jumps over the lazy cog" 0x4e63d2adl;
  ()

let routing_shard ~nr_shards ?partition_size ?routing id =
  let murmur3_hash x = Unsigned.UInt32.of_int (Int32.to_int (murmur3_hash x)) in
  let hash = murmur3_hash (Option.default id routing) in
  let offset =
    match partition_size with
    | Some size when size > 1 -> Unsigned.UInt32.rem (murmur3_hash id) (Unsigned.UInt32.of_int size)
    | _ -> Unsigned.UInt32.zero
  in
  Unsigned.UInt32.rem (Unsigned.UInt32.add hash offset) (Unsigned.UInt32.of_int nr_shards) |> Unsigned.UInt32.to_int

let _routing_shard_test =
  let open OUnit in
  "routing_shard" >:: fun () ->
  let t ?(nr_shards = 8) ?partition_size ?routing id expected =
    let routing_shard = routing_shard ~nr_shards ?partition_size ?routing id in
    OUnit.assert_equal ~printer:string_of_int routing_shard expected
  in
  t "sEERfFzPSI" 1;
  t "cNRiIrjzYd" 7;
  t "BgfLBXUyWT" 5;
  t "cnepjZhQnb" 3;
  t "OKCmuYkeCK" 6;
  t "OutXGRQUja" 5;
  t "yCdyocKWou" 1;
  t "KXuNWWNgVj" 2;
  t "DGJOYrpESx" 4;
  t "upLDybdTGs" 5;
  t "yhZhzCPQby" 1;
  t "EyCVeiCouA" 1;
  t "tFyVdQauWR" 6;
  t "nyeRYDnDQr" 6;
  t "hswhrppvDH" 0;
  t "BSiWvDOsNE" 5;
  t "YHicpFBSaY" 1;
  t "EquPtdKaBZ" 4;
  t "rSjLZHCDfT" 5;
  t "qoZALVcite" 7;
  t "yDCCPVBiCm" 7;
  t "ngizYtQgGK" 5;
  t "FYQRIBcNqz" 0;
  t "EBzEDAPODe" 2;
  t "YePigbXgKb" 1;
  t "PeGJjomyik" 3;
  t "cyQIvDmyYD" 7;
  t "yIEfZrYfRk" 5;
  t "kblouyFUbu" 7;
  t "xvIGbRiGJF" 3;
  t "KWimwsREPf" 4;
  t "wsNavvIcdk" 7;
  t "xkWaPcCmpT" 0;
  t "FKKTOnJMDy" 7;
  t "RuLzobYixn" 2;
  t "mFohLeFRvF" 4;
  t "aAMXnamRJg" 7;
  t "zKBMYJDmBI" 0;
  t "ElSVuJQQuw" 7;
  t "pezPtTQAAm" 7;
  t "zBjjNEjAex" 2;
  t "PGgHcLNPYX" 7;
  t "hOkpeQqTDF" 3;
  t "chZXraUPBH" 7;
  t "FAIcSmmNXq" 5;
  t "EZmDicyayC" 0;
  t "GRIueBeIyL" 7;
  t "qCChjGZYLp" 3;
  t "IsSZQwwnUT" 3;
  t "MGlxLFyyCK" 3;
  t "YmscwrKSpB" 0;
  t "czSljcjMop" 5;
  t "XhfGWwNlng" 1;
  t "cWpKJjlzgj" 7;
  t "eDzIfMKbvk" 1;
  t "WFFWYBfnTb" 0;
  t "oDdHJxGxja" 7;
  t "PDOQQqgIKE" 1;
  t "bGEIEBLATe" 6;
  t "xpRkJPWVpu" 2;
  t "kTwZnPEeIi" 2;
  t "DifcuqSsKk" 1;
  t "CEmLmljpXe" 5;
  t "cuNKtLtyJQ" 7;
  t "yNjiAnxAmt" 5;
  t "bVDJDCeaFm" 2;
  t "vdnUhGLFtl" 0;
  t "LnqSYezXbr" 5;
  t "EzHgydDCSR" 3;
  t "ZSKjhJlcpn" 1;
  t "WRjUoZwtUz" 3;
  t "RiBbcCdIgk" 4;
  t "yizTqyjuDn" 4;
  t "QnFjcpcZUT" 4;
  t "agYhXYUUpl" 7;
  t "UOjiTugjNC" 7;
  t "nICGuWTdfV" 0;
  t "NrnSmcnUVF" 2;
  t "ZSzFcbpDqP" 3;
  t "YOhahLSzzE" 5;
  t "iWswCilUaT" 1;
  t "zXAamKsRwj" 2;
  t "aqGsrUPHFq" 5;
  t "eDItImYWTS" 1;
  t "JAYDZMRcpW" 4;
  t "lmvAaEPflK" 7;
  t "IKuOwPjKCx" 5;
  t "schsINzlYB" 1;
  t "OqbFNxrKrF" 2;
  t "QrklDfvEJU" 6;
  t "VLxKRKdLbx" 4;
  t "imoydNTZhV" 1;
  t "uFZyTyOMRO" 4;
  t "nVAZVMPNNx" 3;
  t "rPIdESYaAO" 5;
  t "nbZWPWJsIM" 0;
  t "wRZXPSoEgd" 3;
  t "nGzpgwsSBc" 4;
  t "AITyyoyLLs" 4;
  ()

let index_args_of_ranges ranges =
  match ranges with
  | [ (index, shards) ] -> index, [ "preference", "_shards:" ^ Stre.catmap ~sep:"," string_of_int shards ]
  | ranges -> Stre.catmap ~sep:"," fst ranges, []

let make_preference ?shards p =
  (* It is important to keep [_shards] first
     https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-preference.html *)
  let prefs =
    match shards with
    | Some [ (_, shards) ] ->
      let shards = "_shards:" ^ Stre.catmap ~sep:"," string_of_int shards in
      shards :: p
    | Some _ | None -> p
  in
  match prefs with
  | [] -> []
  | prefs -> [ "preference", String.concat "|" prefs ]

module Scroller (S : Scroll_hit) : Scroller with type t = S.t = struct
  module Hidden = struct let log = Log.from "scroll_worker" end

  open Hidden

  type t = S.t

  type process_batch_result =
    | Continue
    | Stop
    | Restart

  type retry = {
    max_delay : Time.t;
    max_retries : int option;
  }

  let default_retry = { max_delay = Time.seconds 10; max_retries = None }

  type process_batch = int option * S.t list -> process_batch_result Lwt.t

  exception Wrapped of exn

  let scroll_batch_lwt ~hosts ?request_timeout ?(preference = []) ?(args = []) ?(name = "scroll") ?(verbose = false)
    ?(timeout = Time.minutes 2) ?setup ?size ?limit ?ranges ~index ?kind ?(on_scroll_failed = default_retry) ~make_query
    (process_batch : process_batch)
    =
    let timeout = Time.int timeout in
    let elapsed = new Action.timer in
    let total_hits = ref 0 in
    let processed_hits = ref 0 in
    let stats = CC.create () in
    let tick =
      Action.timely (Time.minutes 1) (fun _ ->
        log#info "%s elapsed %s performed %d total %d (%d%%)" name elapsed#get_str !processed_hits !total_hits
          ( match !total_hits with
          | 0 -> 0
          | total -> !processed_hits * 100 / total
          );
        log#info "%s stats: %s" name (CC.show stats ~sep:", " id)
      )
    in
    let index =
      match ranges with
      | None -> index
      | Some ranges -> fst (index_args_of_ranges ranges)
    in
    let prefs = make_preference ?shards:ranges preference in
    if verbose then
      log#info "%s using prefs parameter %S and argument %S" name (Web.make_url_args prefs) (Web.make_url_args args);
    let args = prefs @ args in
    let rec loop_scroll attempt () =
      match make_query () with
      | exception exn ->
        log#error ~exn "%s make_query" name;
        Lwt.fail exn
      | query ->
        let query = Elastic_query_dsl.top_query_to_string query in
        log#info "%s start on indices %s with query %s" name index query;
        ( try%lwt
            scroll_lwt ~hosts ~index ?kind ?request_timeout ~verbose:true ~query ~timeout ?setup ?size ~args
              (fun ~scroll ~close s ->
              tick ();
              match S.unformat s with
              | exception exn ->
                log#warn ~exn "%s invalid scroll result : %s" name (Stre.shorten 4000 s);
                CC.add stats "parse_error";
                Lwt.fail exn
              | { Elastic_t.scroll_id; shards; _ } as response ->
                let failed_shards =
                  match shards with
                  | Some { Elastic_t.total; successful; failed } when failed > 0 ->
                    log#warn "%s shards total %d successful %d failed %d" name total successful failed;
                    Some failed
                  | Some _ -> Some 0
                  | None -> None
                in
                ( match response with
                | _ when Daemon.should_exit () ->
                  let%lwt () = close scroll_id in
                  Lwt.fail Daemon.ShouldExit
                | { Elastic_t.error = Some error; _ } ->
                  CC.add stats (sprintf "scroll_error_%s" error.Elastic_t.error);
                  let%lwt () = close scroll_id in
                  Exn_lwt.fail "scroll error : %s (%s)" error.error (Option.default "" error.reason)
                | { hits = Some { hits = []; _ }; _ } ->
                  log#info "%s end scroll" name;
                  close scroll_id
                | { hits = Some x; _ } ->
                  total_hits := x.total.value;
                  let len = List.length x.hits in
                  if verbose then
                    log#info "%s scroll got hits %d processed %d total %a limit %s" name len !processed_hits
                      Wrap.Total.sprint x.total
                      (Option.map_default string_of_int "NONE" limit);
                  let hits, len, continue =
                    match limit with
                    | Some limit when !processed_hits + len > limit ->
                      let n = limit - !processed_hits in
                      log#info "%s scroll reached the limit of %d hits. %d hits will be processed" name limit n;
                      let hits = List.take n x.hits in
                      hits, n, false
                    | _ -> x.hits, len, true
                  in
                  processed_hits += len;
                  ( match%lwt process_batch (failed_shards, hits) with
                  | exception exn ->
                    log#error ~exn "%s process_batch index %s" name index;
                    let%lwt () = close scroll_id in
                    Lwt.fail (Wrapped exn)
                  | _ when not continue -> close scroll_id
                  | Continue -> scroll scroll_id
                  | Stop -> close scroll_id
                  | Restart ->
                    let%lwt () = close scroll_id in
                    loop_scroll attempt ()
                  )
                | _ ->
                  log#info "%s end scroll" name;
                  close scroll_id
                )
            )
          with
        | ((Daemon.ShouldExit | Lwt.Canceled) as exn) | Wrapped exn ->
          log#warn ~exn "%s failed index %s" name index;
          Lwt.fail exn
        | exn ->
          log#warn ~exn "%s scroll failed" name;
          let { max_delay; max_retries; _ } = on_scroll_failed in
          let%lwt attempt = Retry.exp_backoff ~exn ~name ?max_retries ~max_delay attempt in
          loop_scroll attempt ()
        )
    in
    let%lwt () = loop_scroll 1 () in
    log#info "%s finished index %s" name index;
    Lwt.return_unit

  let scroll_stream_lwt' ~hosts ?name ?verbose ?request_timeout ?(on_shard_failure = `Continue) ?args ?preference
    ?timeout ?setup ?size ?limit ?ranges ~index ?kind ?on_scroll_failed ~make_query push
    =
    let process_batch (failed_shards, hits) =
      let%lwt () = Lwt_list.iter_s push hits in
      match failed_shards with
      | Some 0 | None -> Lwt.return Continue
      | Some failed_shards ->
      match on_shard_failure with
      | `Continue -> Lwt.return Continue
      | `Stop -> Lwt.return Stop
      | `Restart f ->
        log#warn "restarting %s because of %d failed shards" (Option.default "scroll" name) failed_shards;
        f ();
        Lwt.return Restart
      | `Fail exn -> Lwt.fail exn
    in
    scroll_batch_lwt ~hosts ?name ?verbose ?request_timeout ?args ?preference ?timeout ?setup ?size ?limit ?ranges
      ~index ?kind ?on_scroll_failed ~make_query process_batch

  let scroll_stream_lwt ~hosts ?name ?verbose ?request_timeout ?on_shard_failure ?args ?preference ?timeout ?setup ?size
    ?limit ?ranges ~index ?kind ?on_scroll_failed ~query push
    =
    scroll_stream_lwt' ~hosts ?name ?verbose ?request_timeout ?on_shard_failure ?args ?preference ?timeout ?setup ?size
      ?limit ?ranges ~index ?kind ?on_scroll_failed ~make_query:(const query) push

  let scroll_worker_lwt ~hosts ?name ?(verbose = false) ?request_timeout ?on_shard_failure ?args ?(queue_size = 5000)
    ?(batch_size = 100) ?timeout ?setup ?size ?limit ?ranges ~index ?kind ?on_scroll_failed ~query f
    =
    let worker_s, push = Lwt_stream.create_bounded queue_size in
    let rec worker () =
      match%lwt Lwt_stream.nget batch_size worker_s with
      | [] ->
        log#info "worker finished";
        Lwt.return_unit
      | elems ->
        if verbose then log#info "worker received %d elements" (List.length elems);
        let%lwt () =
          try%lwt f elems
          with exn ->
            log#error ~exn "uncaugth exception in worker loop";
            Lwt.fail exn
        in
        worker ()
    in
    let scroller () =
      (scroll_stream_lwt ~hosts ?name ~verbose ?request_timeout ?on_shard_failure ?args ?timeout ?setup ?size ?limit
         ?ranges ~index ?kind ?on_scroll_failed ~query push#push
      )
        [%finally
          push#close;
          Lwt.return_unit]
    in
    let%lwt () = worker ()
    and () = scroller () in
    Lwt.return_unit
end

(** Escape the characters reserved in a query string.
    The list is specified at
    https://www.elastic.co/guide/en/elasticsearch/reference/6.6/query-dsl-query-string-query.html#_reserved_characters
    ['<' '>'] can't be escaped and should be removed.
*)
let escape_query_string s =
  String.replace_chars
    (function
      | ( '+' | '-' | '=' | '>' | '<' | '!' | '(' | ')' | '{' | '}' | '[' | ']' | '^' | '"' | '~' | '*' | '?' | ':'
        | '\\' | '/' | '&' | '|' ) as c ->
        "\\" ^ String.of_char c
      | c -> String.of_char c
      )
    s
