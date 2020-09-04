import papermill as pm

pm.execute_notebook('notebooks/Analysis.ipynb', 'notebooks/Report.ipynb', parameters={'f_name':'df_final.gzip'})