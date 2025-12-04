import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, when_matched

st.title("Pending Smoothie Orders")
st.write("Orders that need to be filled!")

session = get_active_session()

# Load data into a Pandas dataframe for st.data_editor
df = session.table("smoothies.public.orders") \
            .filter(col("ORDER_FILLED") == 0) \
            .to_pandas()

if not df.empty:

    editable_df = st.data_editor(df, use_container_width=True)

    submitted = st.button("Submit")

    if submitted:

        # Convert back into Snowpark dataframe
        edited_dataset = session.create_dataframe(editable_df)

        og_dataset = session.table("smoothies.public.orders")

        try:
            # Perform merge
            og_dataset.merge(
                edited_dataset,
                og_dataset["ORDER_UID"] == edited_dataset["ORDER_UID"],
                [
                    when_matched().update(
                        {"ORDER_FILLED": edited_dataset["ORDER_FILLED"]}
                    )
                ]
            ).collect()  # IMPORTANT: triggers execution
