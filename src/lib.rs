use darling::FromMeta;
use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, AttributeArgs, Data, DataStruct, DeriveInput, Fields};

// The macro attributes
#[derive(FromMeta)]
struct Args {
    table_name: String,
}

// A function to help parse macro attributes
fn parse_args<ArgStruct>(args: AttributeArgs) -> Result<ArgStruct, TokenStream>
where
    ArgStruct: FromMeta,
{
    ArgStruct::from_list(&args).map_err(|err| err.write_errors().into())
}

// The macro itself
#[proc_macro_attribute]
pub fn model(attributes: TokenStream, item: TokenStream) -> TokenStream {
    // Parse the attributes
    let attributes = parse_macro_input!(attributes as AttributeArgs);
    let args: Args = parse_args(attributes).unwrap();

    // Parse the input
    let input = parse_macro_input!(item as DeriveInput);
    // Get the input from the struct name
    let struct_name = &input.ident;
    // Get all fields
    let fields = match &input.data {
        Data::Struct(DataStruct {
            fields: Fields::Named(fields),
            ..
        }) => &fields.named,
        _ => panic!("expected a struct with named fields"),
    };
    // Collect the field names in both a string and ident version
    let field_name = fields.iter().map(|field| &field.ident);
    let field_name_parsed = fields
        .iter()
        .map(|field| field.ident.as_ref().unwrap().to_string());

    // Construct the necessary cql queries with the given parameters
    let find_input_by_id_cql = format!(
        "SELECT * FROM {} WHERE id = ? ALLOW FILTERING;",
        args.table_name
    );
    let find_input_by_column_cql = format!(
        "SELECT * FROM {} WHERE {{}} = ? ALLOW FILTERING;",
        args.table_name
    );
    let query_values_cql = format!(
        "INSERT INTO {} ({}) VALUES ({});",
        args.table_name,
        fields
            .iter()
            .map(|field| field.ident.as_ref().unwrap().to_string())
            .collect::<Vec<String>>()
            .join(", "),
        fields.iter().map(|_| "?").collect::<Vec<&str>>().join(", ")
    );
    let delete_cql = format!("DELETE FROM {} WHERE id = ?;", args.table_name);

    // Construct the output
    let output = quote! {
            #input

             impl Model for #struct_name {
        fn find_by_id<T>(
            connection: &Connection,
            id: T,
        ) -> std::result::Result<Option<Box<Self>>, Box<dyn std::error::Error>>
        where
            T: Into<Value>,
        {
            let cql = #find_input_by_id_cql;

            let rows = connection
                .session
                .query_with_values(cql, query_values!(id))?
                .get_body()?
                .into_rows();

            match rows {
                Some(mut rows) if !rows.is_empty() => {
                    let row = rows.remove(0);
                    let instance = Self::try_from_row(row)?;

                    Ok(Some(Box::new(instance)))
                }
                _ => Ok(None),
            }
        }

        fn find_by_column<T, U>(
            connection: &Connection,
            column: T,
            value: U,
        ) -> std::result::Result<Vec<Box<Self>>, Box<dyn std::error::Error>>
        where
            T: Display,
            U: Into<Value> + Display,
        {
            let cql = format!(
                #find_input_by_column_cql,
                column
            );

            let rows = connection
                .session
                .query_with_values(cql, query_values!(value))?
                .get_body()?
                .into_rows()
                .or(Some(vec![]))
                .ok_or(SimpleError::new("Failed to retrieve data"))?;

            let mut instances: Vec<Box<Self>> = vec![];

            for row in rows.into_iter() {
                let instance = Self::try_from_row(row)?;
                instances.push(Box::new(instance))
            }

            Ok(instances)
        }

        fn save(
            mut self,
            connection: &Connection,
        ) -> std::result::Result<(), Box<dyn std::error::Error>> {
            let current_time = Utc::now();
            let current_time_spec = Timespec {
                sec: current_time.timestamp(),
                nsec: current_time.timestamp_subsec_nanos() as i32,
            };
            self.updated_at = current_time_spec;

            let insert = #query_values_cql;
            connection
                .session
                .query_with_values(insert, self.into_query_values())?;

            Ok(())
        }

        fn into_query_values(self) -> QueryValues {
            query_values!(
            #(
                    #field_name_parsed => self.#field_name
                ),*
            )
        }

        fn delete(
            self,
            connection: &Connection,
        ) -> std::result::Result<(), Box<dyn std::error::Error>> {
            let delete = #delete_cql;
            connection
                .session
                .query_with_values(delete, query_values!(self.id))?;

            Ok(())
        }

         pub async fn from_rows(rows: Option<Vec<Row>>) -> Result<Vec<Self>, Box<dyn Error>> {
        let mut instances = vec!();

        if let Some(rows) = rows {
            for row in rows.into_iter() {
                let instance = Self::try_from_row(row)?;
                instances.push(instance)
            }
        }

        Ok(instances)
    }
    }
        };

    // Return the output as a token stream
    TokenStream::from(output)
}
