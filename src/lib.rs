use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Data, DataStruct, DeriveInput, Fields, AttributeArgs, NestedMeta, Lit};

#[proc_macro_attribute]
pub fn model(attr: TokenStream, item: TokenStream) -> TokenStream {
    let mut attrs = parse_macro_input!(attr as AttributeArgs);

    let input = parse_macro_input!(item as DeriveInput);

    let struct_name = &input.ident;

    let fields = match &input.data {
        Data::Struct(DataStruct {
            fields: Fields::Named(fields),
            ..
        }) => &fields.named,
        _ => panic!("expected a struct with named fields"),
    };

    let table_name = match attrs.remove(0) {
        NestedMeta::Lit(l) => match l {
            Lit::Str(s) => s.value(),
            _ => panic!("Invalid arguments 1")
        }
        NestedMeta::Meta(_) => panic!("Invalid arguments")
    };

    let field_name = fields.iter().map(|field| &field.ident);
    let field_name_parsed = fields.iter().map(|field| field.ident.as_ref().unwrap().to_string());

    let find_input_by_id_cql = format!("SELECT * FROM {} WHERE id = ? ALLOW FILTERING", table_name);
    let find_input_by_column_cql = format!("SELECT * FROM {} WHERE {{}} = ? ALLOW FILTERING", table_name);
    let query_values_cql = format!("INSERT INTO {} ({}) VALUES ({});", table_name, fields.iter().map(|field| field.ident.as_ref().unwrap().to_string()).collect::<Vec<String>>().join(", "), fields.iter().map(|_| "?").collect::<Vec<&str>>().join(", "));
    let delete_cql = format!("DELETE FROM {} WHERE id = ?;", table_name);

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
    }
        };

    TokenStream::from(output)
}
