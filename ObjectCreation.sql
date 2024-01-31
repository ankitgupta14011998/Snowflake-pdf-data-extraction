use role sysadmin;

create or replace database pdf;
create or replace warehouse quickstart;

use database pdf;
use schema public;
use warehouse quickstart;

create or replace stage pdf_external
url="s3://sfquickstarts/Analyze PDF Invoices/Invoices/"
directory = (enable = TRUE); --Specifies whether to add a directory table to the stage. When the value is TRUE, a directory table is created with the stage.

ls @pdf_external;

-- Create a java function to parse PDF files
create or replace function python_read_pdf(file string)
    returns string
    language python
    runtime_version = 3.8
    packages = ('snowflake-snowpark-python','pypdf2')
    handler = 'read_file'
as
$$
from PyPDF2 import PdfFileReader
from snowflake.snowpark.files import SnowflakeFile
from io import BytesIO
def read_file(file_path):
    whole_text = ""
    with SnowflakeFile.open(file_path, 'rb') as file:
        f = BytesIO(file.readall())
        pdf_reader = PdfFileReader(f)
        whole_text = ""
        for page in pdf_reader.pages:
            whole_text += page.extract_text()
    return whole_text
$$;

select python_read_pdf(build_scoped_file_url(@pdf_external,'invoice1.pdf')) 
as pdf_text;

create or replace table python_parsed_pdf as
select
    relative_path
    , file_url
    , python_read_pdf(build_scoped_file_url(@pdf_external, relative_path)) as parsed_text
from directory(@pdf_external);

select * from python_parsed_pdf;
create or replace view v__parsed_pdf_fields as (
with items_to_array as (
    select
            parsed_text
            , regexp_substr_all(
                substr(
                    regexp_substr(parsed_text, 'Amount\n(.*)\n(.*)\n(.*)\n(.*)\n(.*)\n(.*)\n(.*)\n(.*)\n(.*)\n(.*)\n(.*)\n(.*)'
                    ), 8
                ), '[^\n]+\n[^\n]+\n[^\n]+\n[^\n]+'
            )
        as items
    from python_parsed_pdf
)
, parsed_pdf_fields as (
    select
        substr(regexp_substr(parsed_text, '# [0-9]+'), 2)::int as invoice_number
        , to_number(substr(regexp_substr(parsed_text, '\\$[^A-Z]+'), 2), 10, 2) as balance_due
        , substr(
            regexp_substr(parsed_text, '[0-9]+\n[^\n]+')
                , len(regexp_substr(parsed_text, '# [0-9]+'))
            ) as invoice_from
        , to_date(regexp_substr(parsed_text, '([A-Za-z]+ [0-9]+, [0-9]+)'), 'mon dd, yyyy') as invoice_date
        , i.value::string as line_item
        , parsed_text
    from
        items_to_array
        , lateral flatten(items_to_array.items) i
)
select
    invoice_number
    , balance_due
    , invoice_from
    , invoice_date
    , regexp_substr(line_item, '\n[0-9]+\n')::integer as item_quantity
    , to_number(ltrim(regexp_substr(line_item, '\\$[^\n]+')::string, '$'), 10, 2) as item_unit_cost
    , regexp_substr(line_item, '[^\n]+', 1, 1)::string as item_name
    , to_number(ltrim(regexp_substr(line_item, '\\$[^\n]+', 1, 2)::string, '$'), 10, 2) as item_total_cost
from parsed_pdf_fields
);
