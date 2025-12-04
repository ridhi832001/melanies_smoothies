import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, when_matched

st.title("Pending Smoothie Orders")

st.write("Orders that need to be filled!")

session = get_active_session()
my_dataframe = session.table("smoothies.public.orders").filter(col("ORDER_FILLED")==0).collect()

if my_dataframe:
 editable_df = st.data_editor(my_dataframe)
 submitted =st.button('submit')
 if submitted:
    
    og_dataset = session.table("smoothies.public.orders")
    edited_dataset = session.create_dataframe(editable_df)
    
    try:
        og_dataset.merge(edited_dataset
                     , (og_dataset['ORDER_UID'] == edited_dataset['ORDER_UID'])
                     , [when_matched().update({'ORDER_FILLED': edited_dataset['ORDER_FILLED']})]
        
                    )
        st.success('order placed', icon = '👍')

    except:
        st.write('something went wrong')
else:
    st.success('Someone clicked the button', icon = '👍')

