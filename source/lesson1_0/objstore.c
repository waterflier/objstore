
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <inttypes.h>
#include <string.h>

#include <sys/stat.h>

typedef struct tagObjStoreContext {
    uint8_t utf8RootDir[256];
} ObjStoreContext;

//工具函数

//一次读出文件的所有内容，当pdataLength的长度不够时，会返回必须的长度
int ReadFileContent(FILE* pf,uint8_t* pOutput,uint64_t* pDataLength)
{
	uint64_t readpos = 0;
	uint64_t bufferlen = 0;

	fseek(pf, 0, SEEK_END);
	bufferlen = ftell(pf);
	if (bufferlen > *pDataLength)
	{
		*pDataLength = bufferlen;
		return -1;
	}
	fseek(pf, 0, SEEK_SET);
	bufferlen = *pDataLength;

	do
	{
		uint64_t readlen = fread(pOutput + readpos, 1, bufferlen - readpos, pf);
		if (readlen > 0)
		{
			readpos += readlen;
			printf("read %d bytes\n", readlen);
		}
		else if (readlen == 0)
		{
			break;
		}
		else
		{
			return -1;
		}
	} while (!feof(pf));

	*pDataLength = readpos;
	return 0;
}

int WriteFileContent(FILE* pf,const uint8_t* pData, uint64_t dataLen)
{
	uint64_t writepos = 0;
	while (writepos < dataLen)
	{
		uint64_t writelen = fwrite(pData + writepos, 1, dataLen - writepos, pf);
		printf("write %d bytes\n", writelen);
		if (writelen > 0)
		{
			writepos += writelen;
		}
		else
		{
			return -1;
		}
	}
	
	return 0;
}

int MakeDirPath(const char* utf8Path)
{

}



//项目代码


void GetObjPath(ObjStoreContext* pContext,uint64_t objid,uint8_t* pOutput)
{
    sprintf(pOutput,"%s%d.dat",pContext->utf8RootDir,objid);
    return ;
}

int InitObjStore(ObjStoreContext* pContext,const uint8_t* utf8RootDir)
{
    if(strlen((const char*)utf8RootDir) >= 256)
    {
        printf("dir too long\n");
        return -1;
    }

    strcpy((char*)(pContext->utf8RootDir),(const char*)utf8RootDir);
    return 0;
}

int IsObjExist(ObjStoreContext* pContext,uint64_t objid)
{
    uint8_t utf8Path[257] = {0};
    GetObjPath(pContext,objid,utf8Path);
    struct stat ret;
    return stat(utf8Path, &ret);
}

int SearhObj(ObjStoreContext* pContext,uint64_t minObjID,uint64_t maxObjID)
{
    return 0;
}

int ReadObjData(ObjStoreContext* pContext,uint64_t objid,uint8_t* pOutput,uint64_t* pDataLen)
{
    uint8_t utf8Path[257] = {0};
    GetObjPath(pContext,objid,utf8Path);   
    FILE* pf = fopen(utf8Path,"rb");
    if (pf)
    {
        uint64_t readpos = 0;
        uint64_t bufferlen = *pDataLen;
        do
        {
            if(bufferlen-readpos < 0)
            {
                //读取空间不足，返回需要的大小
                fseek(pf,0,SEEK_END);
                *pDataLen = ftell(pf);
                fclose(pf);
                return -1;
            }

            uint64_t readlen = fread(pOutput+readpos,1,bufferlen-readpos,pf);
            printf("read %d bytes\n",readlen);
            if(readlen > 0)
            {
                readpos += readlen;
            }
            else if(readlen < 0)
            {
                printf("read error.\n");
                fclose(pf);
                return -1;
            }
            else 
            {
                break;
            }
        }while(!feof(pf));

        *pDataLen = readpos;
        fclose(pf);
        return 0;
    } 
    else
    {
        *pDataLen = 0;
    }

    return -1;
}

int UpdateObjData(ObjStoreContext* pContext,uint64_t objid,const uint8_t* pData,uint64_t dataLen)
{
    uint8_t utf8Path[257] = {0};
    GetObjPath(pContext,objid,utf8Path);   
    FILE* pf = fopen(utf8Path,"wb");
    if (pf)
    {
        uint64_t writepos = 0;
        while(writepos < dataLen)
        { 
            uint64_t writelen = fwrite(pData+writepos,1,dataLen-writepos,pf);
            printf("write %d bytes\n",writelen);
            if(writelen > 0)
            {
                writepos += writelen;
            }
            else
            {
                printf("write error.\n");
                fclose(pf);
                return -1;
            }
        }
        fclose(pf);
        return 0;
    }     

    return -1;
}

int RemoveObj(ObjStoreContext* pContext,uint64_t objid)
{
    uint8_t utf8Path[257] = {0};
    GetObjPath(pContext,objid,utf8Path);   
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
    InitObjStore(pContext,"/tmp/");
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

	return 0;
}

int main(int argc,char** argv)
{
    printf("start\n");

 //   FILE* pf = fopen("G:\\BTCloud\\tumblr_nrh9kdHHcn1uatgt2.mp4","rb");
	//if (pf) 
	//{
	//	uint64_t buffernlen = 1024*1024*128;
	//	uint8_t * buffer = (uint8_t*)malloc(buffernlen);
	//	int ret = ReadFileContent(pf, buffer, &buffernlen);
	//	printf("read file content,ret=%d,len=%d\n", ret, buffernlen);
	//	fclose(pf);

	//	FILE* pf2 = fopen("d:\\tmp\\1.mp4", "wb");
	//	ret = WriteFileContent(pf2, buffer, buffernlen);
	//	printf("write file content,ret=%d,len=%d\n", ret, buffernlen);
	//}

 //   exit(0);

    ObjStoreContext* pContext = (ObjStoreContext*) malloc(sizeof(ObjStoreContext));
    InitObjStore(pContext,"/tmp/");
    testWrite(pContext,10,0x3f,40960000);
    testRead(pContext,10,0x3f,4096000);
    //testCheck(pContext,10,409600);

    return 0;
}
