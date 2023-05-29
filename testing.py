import util
import transform_data_to_df as trf

df = trf.transform('mls_temp.json')
var = [1128684, 1249295, 1251754]
print(df.query("mls_num == [1128684, 1249295, 1251754]"))
# print(df.head())


# print(util.is_mls_num_data_unchanged())