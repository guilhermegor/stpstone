import pandas as pd
from stpstone.utils.parsers.folders import DirFilesManagement

list_ser = list()
list_f = DirFilesManagement().loop_files_w_rule("data", "anbima-avl-funds-pg-*", False, False)

for path_f in list_f:
    complete_path_f = rf"data/{path_f}"
    reader = pd.read_csv(complete_path_f, sep=",")
    df_ = pd.DataFrame(reader)
    list_ser.extend(df_.to_dict(orient="records"))

df_cons_1 = pd.DataFrame(list_ser)
df_cons_1.rename(columns={
    "URL_ANBIMA_FUND": "URL"
}, inplace=True)

reader = pd.read_csv(r"data/anbima-avl-funds-cons.csv", sep=",")
df_cons_2 = pd.DataFrame(reader)
df_cons = pd.concat([df_cons_1, df_cons_2])

df_cons_1.drop_duplicates(inplace=True)
df_cons_2.drop_duplicates(inplace=True)

print(df_cons)
print(df_cons.shape)
print(df_cons_2)
print(df_cons_2.shape)

print(f"QUANTIDADE FUNDOS VÁLIDOS ANBIMA DATA: {df_cons[df_cons["STATUS_CODE"] != 200].shape[0]}")

df_cons_2.to_csv("data/BKP-anbima-avl-funds-cons.csv", index=False)
df_cons.to_csv("data/anbima-avl-funds-cons.csv", index=False)
