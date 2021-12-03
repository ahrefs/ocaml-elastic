open Devkit

let log = Log.from "elastic"

(**
  Build a function to serialize an enum in chunks, returning no more than specified number of bytes each time it is called.
  If a limit_bytes is provided, the only part of enum is consumed that seralizes into not more than the limit_bytes. It is then
  the caller's responsibility to check whether the enum was fully consumed or not after the function returns an empty chunk.
  @param enum to serialize in chunks
  @param make serialization function for the enum
  @param mark function to call for consumed elements
  @param limit_bytes output to the specified number of bytes
  @param strict_limit controls whether the last element that does not fit the limit is included or not
  @return function that takes the maximum size of chunk to return
*)
let chunked_read enum ?limit_bytes ?(strict_limit = false) ?mark make =
  let buf = Buffer.create 1024 in
  let limit = Option.map ref limit_bytes in
  let enum = Enum.map (fun elem -> elem, make elem) enum in
  (* fill up the buffer *)
  let rec fill n =
    match Buffer.length buf, limit with
    | len, _ when len >= n -> ()
    | len, Some limit when len >= !limit -> ()
    | len, _ ->
    match Enum.peek enum with
    | None -> ()
    | Some (elem, str) ->
      let len = len + String.length str in
      ( match limit with
      | Some limit when (strict_limit && len > !limit) || !limit <= 0 -> ()
      | _ ->
        Enum.junk enum;
        call_me_maybe mark elem;
        Buffer.add_string buf str;
        fill n
      )
  in
  fun n ->
    try
      fill n;
      let len = Buffer.length buf in
      let n = min n len in
      ( match limit with
      | Some limit -> limit -= n
      | _ -> ()
      );

      (* flush *)
      let res = Buffer.sub buf 0 n in
      let tail = Buffer.sub buf n (len - n) in
      Buffer.clear buf;

      (* store tail *)
      Buffer.add_string buf tail;
      res
    with exn ->
      log#error ~exn "chunked_read";
      raise exn

(**
   Same as chunked_read, but without trying to return up to limit_bytes at each call.
   Each call will return the string representation of one element,
   or a part of one element if the element string representation is larger than limit_bytes.
 *)
let chunked_read_lax enum ~limit_bytes ?mark make =
  let remaining = ref None in
  let limit = ref limit_bytes in
  let get' () =
    match Enum.get enum with
    | None -> ""
    | Some elem ->
      let str = make elem in
      call_me_maybe mark elem;
      limit -= String.length str;
      str
  in
  let get () =
    match !remaining with
    | Some r ->
      remaining := None;
      r
    | None when !limit <= 0 -> 0, ""
    | None -> 0, get' ()
  in
  fun n ->
    try
      let offset, str = get () in
      let len = String.length str - offset in
      if len <= n then str, offset, len
      else (
        remaining := Some (n + offset, str);
        str, offset, n
      )
    with exn ->
      log#error ~exn "chunked_read";
      raise exn

(**
  Serialize an enum into a string of up to limit_bytes length.
  Only the part of the enum fiting in limit_bytes will be consumed. It is the
  caller's responsibility to check whether the enum was fully consumed or not.
  the caller's
  @param enum to serialize
  @param make serialization function for the enum
  @param mark function to call for consumed elements
  @param limit_bytes output to the specified number of bytes
  @return string representation of the consumed element, not longer than limit_bytes
 *)
let read_up_to enum ?mark ~limit_bytes make =
  let rec loop acc len =
    match len >= limit_bytes with
    | true -> String.concat "" acc
    | false ->
    match Enum.get enum with
    | None -> String.concat "" acc
    | Some elem ->
      let str = make elem in
      Option.may (fun f -> f elem) mark;
      loop (str :: acc) (len + String.length str)
  in
  loop [] 0
