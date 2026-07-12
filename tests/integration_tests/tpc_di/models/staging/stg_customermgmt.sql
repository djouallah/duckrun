{{ config(
    materialized='table',
    pre_hook=["INSTALL webbed FROM community", "LOAD webbed"]
) }}
-- Flattened CustomerMgmt.xml (Batch1 historical customer/account actions).
-- DuckDB has no first-party XML reader, so we use the `webbed` community
-- extension: read_xml_objects loads each file as an XML document, then
-- xml_extract_elements splits out one fragment per <TPCDI:Action> (matched by
-- local name to sidestep the TPCDI: namespace) and xml_extract_text pulls each
-- field. Output mirrors the reference project's CustomerMgmt staging table
-- (phones concatenated, status decoded from ActionType). Materialized as a table
-- so only this model needs the extension; downstream models read plain Delta.
--
-- CustomerMgmt.xml scales with the scale factor (~9MB at sf3, ~300MB at sf100) and
-- webbed silently returns zero rows on a multi-hundred-MB document, so generate_data.py
-- splits it into CustomerMgmt_NNNN.xml chunks (each a valid <TPCDI:Actions> envelope).
-- We read the chunks with the CustomerMgmt_*.xml glob — the '_' excludes any leftover
-- monolithic CustomerMgmt.xml. maximum_file_size is a belt-and-suspenders raise of the
-- 16 MiB default in case a chunk runs slightly larger.
with actions as (
  select unnest(xml_extract_elements(xml, $x$//*[local-name()='Action']$x$)) as node
  from read_xml_objects('{{ var("tpcdi_dir") }}/Batch1/CustomerMgmt_*.xml',
                        maximum_file_size => 2000000000)
),
raw as (
  select
    {{ cm("/*/@ActionType") }} as actiontype,
    {{ cm("/*/@ActionTS") }} as actionts,
    {{ cm("//*[local-name()='Customer']/@C_ID") }} as customerid,
    {{ cm("//*[local-name()='Customer']/@C_TAX_ID") }} as taxid,
    {{ cm("//*[local-name()='Customer']/@C_GNDR") }} as gender,
    {{ cm("//*[local-name()='Customer']/@C_TIER") }} as tier,
    {{ cm("//*[local-name()='Customer']/@C_DOB") }} as dob,
    {{ cm("//*[local-name()='Account']/@CA_ID") }} as accountid,
    {{ cm("//*[local-name()='Account']/@CA_TAX_ST") }} as taxstatus,
    {{ cm("//*[local-name()='CA_B_ID']") }} as brokerid,
    {{ cm("//*[local-name()='CA_NAME']") }} as accountdesc,
    {{ cm("//*[local-name()='C_L_NAME']") }} as lastname,
    {{ cm("//*[local-name()='C_F_NAME']") }} as firstname,
    {{ cm("//*[local-name()='C_M_NAME']") }} as middleinitial,
    {{ cm("//*[local-name()='C_ADLINE1']") }} as addressline1,
    {{ cm("//*[local-name()='C_ADLINE2']") }} as addressline2,
    {{ cm("//*[local-name()='C_ZIPCODE']") }} as postalcode,
    {{ cm("//*[local-name()='C_CITY']") }} as city,
    {{ cm("//*[local-name()='C_STATE_PROV']") }} as stateprov,
    {{ cm("//*[local-name()='C_CTRY']") }} as country,
    {{ cm("//*[local-name()='C_PRIM_EMAIL']") }} as email1,
    {{ cm("//*[local-name()='C_ALT_EMAIL']") }} as email2,
    {{ cm("//*[local-name()='C_LCL_TX_ID']") }} as lcl_tx_id,
    {{ cm("//*[local-name()='C_NAT_TX_ID']") }} as nat_tx_id,
    {{ cm("//*[local-name()='C_PHONE_1']/*[local-name()='C_CTRY_CODE']") }} as p1_ctry,
    {{ cm("//*[local-name()='C_PHONE_1']/*[local-name()='C_AREA_CODE']") }} as p1_area,
    {{ cm("//*[local-name()='C_PHONE_1']/*[local-name()='C_LOCAL']") }} as p1_local,
    {{ cm("//*[local-name()='C_PHONE_1']/*[local-name()='C_EXT']") }} as p1_ext,
    {{ cm("//*[local-name()='C_PHONE_2']/*[local-name()='C_CTRY_CODE']") }} as p2_ctry,
    {{ cm("//*[local-name()='C_PHONE_2']/*[local-name()='C_AREA_CODE']") }} as p2_area,
    {{ cm("//*[local-name()='C_PHONE_2']/*[local-name()='C_LOCAL']") }} as p2_local,
    {{ cm("//*[local-name()='C_PHONE_2']/*[local-name()='C_EXT']") }} as p2_ext,
    {{ cm("//*[local-name()='C_PHONE_3']/*[local-name()='C_CTRY_CODE']") }} as p3_ctry,
    {{ cm("//*[local-name()='C_PHONE_3']/*[local-name()='C_AREA_CODE']") }} as p3_area,
    {{ cm("//*[local-name()='C_PHONE_3']/*[local-name()='C_LOCAL']") }} as p3_local,
    {{ cm("//*[local-name()='C_PHONE_3']/*[local-name()='C_EXT']") }} as p3_ext
  from actions
)
select
  try_cast(customerid as bigint) as customerid,
  try_cast(accountid as bigint) as accountid,
  try_cast(brokerid as bigint) as brokerid,
  nullif(taxid, '') as taxid,
  nullif(accountdesc, '') as accountdesc,
  try_cast(taxstatus as tinyint) as taxstatus,
  case actiontype
    when 'NEW' then 'Active'
    when 'ADDACCT' then 'Active'
    when 'UPDACCT' then 'Active'
    when 'UPDCUST' then 'Active'
    when 'CLOSEACCT' then 'Inactive'
    when 'INACT' then 'Inactive'
  end as status,
  nullif(lastname, '') as lastname,
  nullif(firstname, '') as firstname,
  nullif(middleinitial, '') as middleinitial,
  nullif(gender, '') as gender,
  try_cast(tier as tinyint) as tier,
  try_cast(dob as date) as dob,
  nullif(addressline1, '') as addressline1,
  nullif(addressline2, '') as addressline2,
  nullif(postalcode, '') as postalcode,
  nullif(city, '') as city,
  nullif(stateprov, '') as stateprov,
  nullif(country, '') as country,
  {{ cm_phone('p1_ctry', 'p1_area', 'p1_local', 'p1_ext') }} as phone1,
  {{ cm_phone('p2_ctry', 'p2_area', 'p2_local', 'p2_ext') }} as phone2,
  {{ cm_phone('p3_ctry', 'p3_area', 'p3_local', 'p3_ext') }} as phone3,
  nullif(email1, '') as email1,
  nullif(email2, '') as email2,
  nullif(lcl_tx_id, '') as lcl_tx_id,
  nullif(nat_tx_id, '') as nat_tx_id,
  try_cast(actionts as timestamp) as update_ts,
  actiontype
from raw
