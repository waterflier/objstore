
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <inttypes.h>
#include <string.h>
#include <time.h>

#include <sys/stat.h>

typedef struct tagObjStoreContext {
    uint8_t utf8RootDir[256];
    uint64_t datapos;
} ObjStoreContext;


void GetObjMetaPath(ObjStoreContext* pContext,uint64_t objid,uint8_t* pOutput)
{
    sprintf(pOutput,"%s/meta/%llu.meta",pContext->utf8RootDir,objid);
    return ;
}

void GetNewObjDataPath(ObjStoreContext* pContext,uint8_t* pOutput)
{
    pContext->datapos = pContext->datapos + 1;
    snprintf(pOutput,256,"%s/data/%llu.data",pContext->utf8RootDir,pContext->datapos);
    return;
}

int InitObjStore(ObjStoreContext* pContext,const uint8_t* utf8RootDir)
{
    if(strlen((const char*)utf8RootDir) >= 256)
    {
        printf("dir too long\n");
        return -1;
    }

    strcpy((char*)(pContext->utf8RootDir),(const char*)utf8RootDir);
    pContext->datapos = time(NULL);
    printf("datapos = %d\n",pContext->datapos);
    pContext->datapos = pContext->datapos << 32;
    return 0;
}

int IsObjExist(ObjStoreContext* pContext,uint64_t objid)
{
    uint8_t utf8Path[257] = {0};
    GetObjMetaPath(pContext,objid,utf8Path);
    struct stat ret;
    return stat(utf8Path, &ret);
}

int SearhObj(ObjStoreContext* pContext,uint64_t minObjID,uint64_t maxObjID)
{
	//TODO:
    return 0;
}

int ReadObjData(ObjStoreContext* pContext,uint64_t objid,uint8_t* pOutput,uint64_t* pDataLen)
{
    uint8_t utf8Path[257] = {0};
    GetObjMetaPath(pContext,objid,utf8Path);   
    FILE* pf = fopen(utf8Path,"rb");
    if (pf)
    {
        uint64_t reallen = fread(utf8Path,1,257,pf);
        utf8Path[reallen] = 0;
        fclose(pf);
        FILE* pfdata = fopen(utf8Path,"rb");
        if (pfdata)
        {
            reallen = fread(pOutput,1,*pDataLen,pfdata);
            *pDataLen = reallen;
            fclose(pfdata);
            return 0;
        }
    } 
    
    *pDataLen = 0;
    return -1;
}

int UpdateObjData(ObjStoreContext* pContext,uint64_t objid,const uint8_t* pData,uint64_t dataLen)
{
    uint8_t utf8Path[257] = {0};
    GetNewObjDataPath(pContext,utf8Path);
    FILE* pf = fopen(utf8Path,"wb");
    if (pf)
    {
        uint64_t writelen = fwrite(pData,1,dataLen,pf);
        fclose(pf);
        uint8_t utf8MetaPath[257] = {0};
        GetObjMetaPath(pContext,objid,utf8MetaPath);
        FILE* pmeta = fopen(utf8MetaPath,"wb");
        if(pmeta)
        {
            fwrite(utf8Path,1,strlen(utf8Path),pmeta);
            fclose(pmeta);

            return 0;
        }
    }     

    return -1;
}

int RemoveObj(ObjStoreContext* pContext,uint64_t objid)
{
	//TODO: 如果要删除data文件，要注意先删除meta再删除data
    uint8_t utf8Path[257] = {0};
    GetObjMetaPath(pContext,objid,utf8Path);   
    return remove(utf8Path);
}



void testWrite(ObjStoreContext* pContext,uint16_t maxid,uint16_t mask,size_t datasize)
{
    uint32_t* pdata = (uint32_t*) malloc(datasize);
    for(int i=0;i<maxid;++i)
    {
        for(uint32_t j=0;j<datasize/4;++j)
        {
            pdata[j] = ((uint32_t)mask)<<16 | i;    
        }

        UpdateObjData(pContext,i,pdata,datasize);
        printf("obj %d update ok.\n",i);
    }
    free(pdata);
}

void testRead(ObjStoreContext* pContext,uint16_t maxid,uint16_t mask,size_t datasize)
{
    uint32_t* pdata = (uint32_t*) malloc(datasize);
    for(int i=0;i<maxid;++i) 
    {
        uint64_t readlen = datasize;
        if(ReadObjData(pContext,i,pdata,&readlen) != 0)
        {
            printf("obj %d test failed.readlen error\n",i);
            continue;
        }

        if(readlen == datasize)
        {
            int testok = 1;
            for(uint32_t j=0;j<datasize/4;++j)
            {
                if(pdata[j] != (((uint32_t)mask<<16) | i))
                {
                    testok = 0;
                    printf("obj %d test failed. %d byte error. %x\n",i,j,pdata[j]);
                    break;
                }    
            }
            if(testok)
            {
                printf("obj %d test ok.\n",i);
            }            
        }
    }
    free(pdata);
}

void testCheck(ObjStoreContext* pContext,uint16_t maxid,size_t datasize)
{
    uint32_t* pdata = (uint32_t*) malloc(datasize);
    for(int i=0;i<maxid;++i) 
    {
        uint64_t readlen = datasize;
        if(ReadObjData(pContext,i,pdata,&readlen) != 0)
        {
            printf("obj %d test failed.readlen error\n",i);
            continue;
        }

        if(readlen == datasize)
        {
            for(uint32_t j=0;j<datasize/4;++j)
            {
                if(j>0)
                {
                    if(pdata[j] != pdata[j-1])
                    {
                        printf("obj %d check failed. %d byte error. %x\n",i,j,pdata[j]);
                        break;
                    }  
                }  
            }   
            printf("obj %d check ok.\n",i);        
        }
    }
    free(pdata);    
}


int devtest()
{
    ObjStoreContext* pContext = (ObjStoreContext*) malloc(sizeof(ObjStoreContext));   
    InitObjStore(pContext,"/tmp/objstore/");
    uint8_t buffer[256];
    uint64_t bufferlen = 256;
    UpdateObjData(pContext,100,"123456",6);
    ReadObjData(pContext,100,buffer,&bufferlen);
    printf("%s\n",buffer);
    if(IsObjExist(pContext,100) == 0)
    {
        printf("exist\n");
    }
    //RemoveObj(pContext,100);
    if(IsObjExist(pContext,100) == 0)
    {
        printf("exist\n");
    }
}

void usage()
{
	printf("usage:\n");
	printf("\t-w maxobjid mask datasize\n");
	printf("\t-r maxobjid mask datasize\n");
	printf("\t-c maxobjid datasize\n");
	return;
}

int main(int argc, char** argv)
{
	printf("start\n");

	ObjStoreContext* pContext = (ObjStoreContext*)malloc(sizeof(ObjStoreContext));
	InitObjStore(pContext, "d:/tmp/objstore/");

	if (argc == 1)
	{
		//for debug
		testWrite(pContext, 10, 0x5f, 9097);
		testRead(pContext, 10, 0x5f, 9097);
		testCheck(pContext, 10, 9097);
	}
	else
	{
		if (argc == 5)
		{

			int maxobjid = atoi(argv[2]);
			int mask = atoi(argv[3]);
			int datasize = atoi(argv[4]);

			if (strcmp(argv[1], "-w") == 0)
			{
				printf("start write...\n");
				testWrite(pContext, maxobjid, mask, datasize);
			}
			else if (strcmp(argv[1], "-r") == 0)
			{
				printf("start read...\n");
				testRead(pContext, maxobjid, mask, datasize);
			}
			else
			{
				usage();
				return 1;
			}
		}
		else if (argc == 4)
		{
			int maxobjid = atoi(argv[2]);
			int datasize = atoi(argv[3]);
			if (strcmp(argv[1], "-c") == 0)
			{
				printf("start check...\n");
				testCheck(pContext, maxobjid, datasize);
			}
			else
			{
				usage();
				return 1;
			}
		}
		else
		{
			usage();
			return 1;
		}
	}


	system("pause");
	return 0;
}