import pandas as pd
import string

chunksOfFile = pd.read_csv(encoding='utf-8',filepath_or_buffer='mappingbased-objects-uncleaned.ttl.bz2',chunksize=1000,
                           compression='bz2',header=None,skiprows=1,sep=' ')
czechDF = pd.DataFrame()

for i in range(10000):
    if i == 1000:
        break
    try:
        chunk = chunksOfFile.get_chunk()
    except:
        continue
    print('Chunk: ', i, " of ")
    chunk.to_csv(path_or_buf='res'+str(i),sep='\t')

#czechDF.to_csv(path_or_buf='czech'+str(i)+'.csv',sep=' ')