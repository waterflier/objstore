
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <inttypes.h>
#include <string.h>
#include <time.h>

#include <sys/stat.h>

//工具函数

//一次读出文件的所有内容，当pdataLength的长度不够时，会返回必须的长度
//#pf的内容不能并行修改
int ReadFileContent(FILE* pf, uint8_t* pOutput, uint64_t* pDataLength)
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
		uint64_t readlen = fread(pOutput + readpos, 1, bufferlen - readpos, pf);//#pf的内容不能并行修改
		if (readlen > 0)
		{
			readpos += readlen;
			//printf("read %d bytes\n", readlen);
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
//#pf的内容不能并行修改
int ReadFileContentWithPos(FILE* pf, uint8_t* pOutput, uint64_t* pDataLength,uint64_t startpos)
{
	uint64_t readpos = 0;
	uint64_t bufferlen = 0;

	fseek(pf, 0, SEEK_END);
	bufferlen = ftell(pf);
	if (bufferlen > *pDataLength + startpos)
	{
		*pDataLength = bufferlen-startpos;
		return -1;
	}
	fseek(pf, startpos, SEEK_SET);
	bufferlen = *pDataLength;

	do
	{
		uint64_t readlen = fread(pOutput + readpos, 1, bufferlen - readpos, pf);//#pf的内容不能并行修改
		if (readlen > 0)
		{
			readpos += readlen;
			//printf("read %d bytes\n", readlen);
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

//#pf的内容不能并行修改
int WriteFileContent(FILE* pf, const uint8_t* pData, uint64_t dataLen)
{
	uint64_t writepos = 0;
	while (writepos < dataLen)
	{
		uint64_t writelen = fwrite(pData + writepos, 1, dataLen - writepos, pf);//#pf的内容不能并行修改
		//printf("write %d bytes\n", writelen);
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

///////////////////////////////////////////////////////////////
// meta->pagelist->page
//               ->page
//               ->page
//               ->page
//
typedef struct tagObjStoreContext {
	uint8_t utf8RootDir[256];
	uint64_t datapos;
	uint32_t pagesize;
} ObjStoreContext;

typedef struct tagPageList
{
	uint32_t listSize;
	uint64_t* pPageArray;
} PageList;

typedef struct tagObjMeta
{
	uint64_t objid;
	uint64_t datalen;
	PageList dataPage;
} ObjMeta;


void ReadPageListFromData(ObjStoreContext* pContext, const uint8_t* pData, PageList* pResult);

//#OK
void GetObjMetaPath(ObjStoreContext* pContext, uint64_t objid, uint8_t* pOutput)
{
	sprintf(pOutput, "%s/meta/%llu.meta", pContext->utf8RootDir, objid);//#utf8RootDir是常量
	return;
}

//#pContext->datapos = pContext->datapos + 1是原子的
void AllocNewPage(ObjStoreContext* pContext, uint8_t* pOutput, uint64_t* pPageIndex)
{
	pContext->datapos = pContext->datapos + 1;//#该操作是原子的
	sprintf(pOutput, "%s/data/%llu.data", pContext->utf8RootDir, pContext->datapos);
	*pPageIndex = pContext->datapos;
	return;
}

//#OK
void GetPagePath(ObjStoreContext* pContext, uint64_t pageIndex, uint8_t* pOutput)
{
	sprintf(pOutput, "%s/data/%llu.data", pContext->utf8RootDir, pageIndex);
	return;
}

//#OK
void GetNewObjPageListDataPath(ObjStoreContext* pContext, uint8_t* pOutput)
{
	pContext->datapos = pContext->datapos + 1;
	sprintf(pOutput, "%s/meta/%llu.page", pContext->utf8RootDir, pContext->datapos);
	return;
}

//#OK
int GetSizeOfObjMeta(ObjStoreContext* pContext, ObjMeta* pMeta, const uint8_t* utf8PageListPath)
{
	return sizeof(uint64_t) * 2 + sizeof(uint32_t) + strlen(utf8PageListPath) + 2;
}

int ReadObjMeta(ObjStoreContext* pContext, uint64_t objid, ObjMeta* pMeta)
{
	uint8_t utf8Path[257] = { 0 };
	GetObjMetaPath(pContext, objid, utf8Path);
	FILE* pf = fopen(utf8Path, "rb");
	if (pf)
	{
		uint64_t bufferlen = 257 + 4 + 16;
		uint8_t metabuffer[257 + 4 + 16] = { 0 };
		
		int readlen = ReadFileContent(pf, metabuffer, &bufferlen);//#MetaFile 够小就是原子操作
		fclose(pf);

		uint64_t* plist = (uint64_t*)metabuffer;
		pMeta->objid = plist[0];
		pMeta->datalen = plist[1];
		uint32_t* pread = (uint32_t*)(metabuffer + 2 * sizeof(uint64_t));
		strncpy(utf8Path, metabuffer + 2 * sizeof(uint64_t) + sizeof(uint32_t), pread[0]);
		FILE* pfPageList = fopen(utf8Path, "rb");
		if (pfPageList)
		{
			uint64_t pageBufferLen = 4 + 8 * (pMeta->datalen / pContext->pagesize) + 8;
			uint8_t* pagebuffer = malloc(pageBufferLen);
			readlen = ReadFileContent(pfPageList, pagebuffer, &pageBufferLen);
			fclose(pfPageList);

			ReadPageListFromData(pContext, pagebuffer, &(pMeta->dataPage));
			free(pagebuffer);

			return 0;//成功
		}
		else
		{
			printf("read page list error\n");
		}
	}
	else
	{
		printf("read meta error\n");
	}

	return -1;
}

void WriteObjMeta(ObjStoreContext* pContext, uint8_t* pData, ObjMeta* pMeta, const uint8_t* pageFilePath)
{
	uint64_t* plist = (uint64_t*)pData;
	plist[0] = pMeta->objid;
	plist[1] = pMeta->datalen;

	uint32_t* pwrite = (uint32_t*)(pData + 2 * sizeof(uint64_t));
	pwrite[0] = strlen(pageFilePath);
	strcpy(pData + 2 * sizeof(uint64_t) + sizeof(uint32_t), pageFilePath);
	//WritePageList(pContext, pData + sizeof(uint64_t) * 2, &(pMeta->dataPage));

	return;
}

int GetSizeOfPageList(PageList* pPageList)
{
	return sizeof(uint32_t) + sizeof(uint64_t) * (pPageList->listSize);
}

void ReadPageListFromData(ObjStoreContext* pContext, const uint8_t* pData, PageList* pResult)
{
	//全读出再解析 or 边读取边解析
	uint32_t* plist = (uint32_t*)pData;
	pResult->listSize = plist[0];
	pResult->pPageArray = (uint64_t*)malloc(sizeof(uint64_t) * pResult->listSize);

	uint32_t i = 0;
	uint64_t* pPageID = (uint64_t*)(plist + 1);
	for (i = 0; i < pResult->listSize; ++i)
	{
		pResult->pPageArray[i] = pPageID[i];
	}

	return;
}

void WritePageList(ObjStoreContext* pContext, uint8_t* pData, PageList* pList)
{
	uint32_t* plist = (uint32_t*)pData;
	plist[0] = pList->listSize;

	uint32_t i = 0;
	uint64_t* pPageID = (uint64_t*)(plist + 1);
	for (i = 0; i < pList->listSize; ++i)
	{
		pPageID[i] = pList->pPageArray[i];
	}

	return;
}

int InitObjStore(ObjStoreContext* pContext, const uint8_t* utf8RootDir)
{
	if (strlen((const char*)utf8RootDir) >= 256)
	{
		printf("dir too long\n");
		return -1;
	}

	strcpy((char*)(pContext->utf8RootDir), (const char*)utf8RootDir);
	pContext->datapos = time(NULL);
	printf("datapos = %I64d\n", pContext->datapos);
	pContext->datapos = pContext->datapos << 32;
	pContext->pagesize = 4096-sizeof(uint64_t);
	return 0;
}

int IsObjExist(ObjStoreContext* pContext, uint64_t objid)
{
	uint8_t utf8Path[257] = { 0 };
	GetObjMetaPath(pContext, objid, utf8Path);
	struct stat ret;
	return stat(utf8Path, &ret);
}

int SearhObj(ObjStoreContext* pContext, uint64_t minObjID, uint64_t maxObjID)
{
	return 0;
}


int ReadObjData(ObjStoreContext* pContext, uint64_t objid, uint8_t* pOutput, uint64_t* pDataLen)
{
	ObjMeta theMeta;
	theMeta.dataPage.pPageArray = NULL;
	int result = -1;
	if (ReadObjMeta(pContext, objid, &theMeta) == 0)
	{
		if (*pDataLen < theMeta.datalen)
		{
			*pDataLen = theMeta.datalen;
			return result;
		}

		int i = 0;
		int readlen = 0;
		uint64_t datalen = theMeta.datalen;
		uint8_t utf8path[257] = { 0 };
		uint8_t* pWritePos = pOutput;
		for (i = 0; i < theMeta.dataPage.listSize; ++i)
		{
			GetPagePath(pContext, theMeta.dataPage.pPageArray[i], utf8path);
			FILE* pf = fopen(utf8path, "rb");
			if (pf)
			{
				uint64_t readlen = datalen + sizeof(uint64_t);
				ReadFileContent(pf, pWritePos, &readlen);
				readlen -= sizeof(uint64_t);
				memcpy(pWritePos, pWritePos + sizeof(uint64_t), readlen);
				pWritePos += readlen;
				datalen -= readlen;
			}
			else
			{
				printf("read page error\n");
				return result;
			}
		}

		*pDataLen = theMeta.datalen;
		result = 0;
	}

	if (theMeta.dataPage.pPageArray)
	{
		free(theMeta.dataPage.pPageArray);
	}

	return result;
}

int ReadObjDataByRange(ObjStoreContext* pContext, uint64_t objid, uint64_t startpos, uint8_t* pOutput, uint64_t* pDataLen)
{
	ObjMeta theMeta;
	int result = -1;
	if (ReadObjMeta(pContext, objid, &theMeta) == 0)
	{
		if (startpos > theMeta.datalen)
		{
			printf("start pos too big!\n");
			return -1;
		}

		int startpage = startpos / pContext->pagesize;
		int endpage = (startpos + *pDataLen) / pContext->pagesize;
		int readlen = 0;

		uint64_t datalen = *pDataLen;
		uint64_t totalread = 0;
		uint8_t utf8path[257] = { 0 };
		uint8_t* pWritePos = pOutput;
		uint8_t* pageBuffer = malloc(pContext->pagesize);

		int i = 0;
		for (i = startpage; i <= endpage; ++i)
		{
			GetPagePath(pContext, theMeta.dataPage.pPageArray[i], utf8path);
			FILE* pf = fopen(utf8path, "rb");
			if (pf)
			{
				if (i == startpage)
				{
					uint64_t readlen = pContext->pagesize;
					ReadFileContentWithPos(pf, pageBuffer, &readlen,sizeof(uint64_t));

					int readpos = startpos % pContext->pagesize;
					int willread = pContext->pagesize - readpos;
					if (willread > *pDataLen)
					{
						willread = *pDataLen;
					}
					memcpy(pWritePos, pageBuffer + readpos, willread);
					pWritePos += willread;
					datalen -= willread;
					totalread += willread;
				}
				else
				{
					uint64_t readlen = datalen;
					ReadFileContentWithPos(pf, pWritePos, &readlen,sizeof(uint64_t));
					pWritePos += readlen;
					datalen -= readlen;
					totalread += readlen;
				}

				fclose(pf);

			}
			else
			{
				printf("read page error\n");
				return result;
			}
		}

		free(pageBuffer);
		*pDataLen = totalread;
		result = 0;
	}

	return result;
}

int UpdateObjData(ObjStoreContext* pContext, uint64_t objid, const uint8_t* pData, uint64_t dataLen)
{
	ObjMeta theMeta;
	theMeta.objid = objid;
	theMeta.datalen = dataLen;

	int pagelen = dataLen / pContext->pagesize;
	if (dataLen % pContext->pagesize != 0)
	{
		pagelen = pagelen + 1;
	}

	theMeta.dataPage.listSize = pagelen;
	theMeta.dataPage.pPageArray = (uint64_t*)malloc(sizeof(uint64_t) * pagelen);

	int i = 0;
	uint64_t writePos = 0;
	for (i = 0; i < pagelen; ++i)
	{
		uint8_t pagepath[257] = { 0 };
		uint64_t pageIndex = 0;
		AllocNewPage(pContext, pagepath, &pageIndex);
		FILE* pfPage = fopen(pagepath, "wb");
		if (pfPage)
		{
			fwrite(&objid, sizeof(uint64_t), 1, pfPage);
			WriteFileContent(pfPage, pData + writePos, dataLen - writePos > pContext->pagesize ? pContext->pagesize : dataLen - writePos);
			fclose(pfPage);
			writePos += pContext->pagesize;
			theMeta.dataPage.pPageArray[i] = pageIndex;
		}
		else
		{
			free(theMeta.dataPage.pPageArray);
			printf("error\n");
		}
	}

	uint8_t utf8Path[257] = { 0 };
	uint8_t utf8MetaPath[257] = { 0 };
	GetNewObjPageListDataPath(pContext, utf8Path);
	FILE* pfPageList = fopen(utf8Path, "wb");
	if (pfPageList)
	{
		int bufferSize = GetSizeOfPageList(&(theMeta.dataPage));
		uint8_t *pBuffer = (uint8_t*)malloc(bufferSize);
		WritePageList(pContext, pBuffer, &(theMeta.dataPage));
		WriteFileContent(pfPageList, pBuffer, bufferSize);
		fclose(pfPageList);
		free(pBuffer);

		GetObjMetaPath(pContext, objid, utf8MetaPath);
		FILE* pfMeta = fopen(utf8MetaPath, "wb");
		if (pfMeta) 
		{
			//用前面的方法，解决meta可能过大的问题
			bufferSize = GetSizeOfObjMeta(pContext, &theMeta, utf8Path);
			pBuffer = (uint8_t*)malloc(bufferSize);
			WriteObjMeta(pContext, pBuffer, &theMeta, utf8Path);
			WriteFileContent(pfMeta, pBuffer, bufferSize);

			free(pBuffer);
			free(theMeta.dataPage.pPageArray);
			fclose(pfMeta);
			return 0;
		}
		else
		{
			printf("write meta error\n");
			free(theMeta.dataPage.pPageArray);
			return -1;
		}
	}
	else
	{
		printf("write pagelist error\n");
		free(theMeta.dataPage.pPageArray);
		return -1;
	}

	free(theMeta.dataPage.pPageArray);
	return -1;
}


int UpdateObjDataByRange(ObjStoreContext* pContext, uint64_t objid, uint64_t startpos, const uint8_t* pData, uint64_t dataLen)
{
	ObjMeta theMeta;
	//theMeta.objid = objid;
	//theMeta.datalen = dataLen;

	int result = -1;
	if (ReadObjMeta(pContext, objid, &theMeta) != 0)
	{
		return -1;
	}

	if (startpos > theMeta.datalen)
	{
		printf("start pos too big!\n");
		return -1;
	}

	int startpage = startpos / pContext->pagesize;
	int endpage = (startpos + dataLen) / pContext->pagesize;

	int i = 0;
	int readlen = 0;
	uint64_t datalen = theMeta.datalen;
	uint8_t utf8path[257] = { 0 };
	uint8_t* pWritePos = pData;
	uint8_t* pageBuffer = malloc(pContext->pagesize);
	uint8_t* willWritePageBuffer = pageBuffer;
	int willWriteLen = pContext->pagesize;
	int pagepos = startpos % pContext->pagesize;
	for (i = startpage; i <= endpage; ++i)
	{
		if (i == startpage)
		{
			//处理头部
			if (pagepos != 0)
			{
				//不是整页
				GetPagePath(pContext, theMeta.dataPage.pPageArray[i], utf8path);
				FILE* pfPage = fopen(utf8path, "rb");
				if (pfPage)
				{
					uint64_t pagesize = pContext->pagesize;
					ReadFileContentWithPos(pfPage, pageBuffer, &pagesize,sizeof(uint64_t));
					fclose(pfPage);
				}
				else
				{
					printf("read page error\n");
					free(pageBuffer);
					return -1;
				}
			}

			int copylen = pagepos + dataLen > pContext->pagesize ? pContext->pagesize - pagepos : dataLen;
			memcpy(pageBuffer + pagepos, pData, copylen);
			willWritePageBuffer = pageBuffer;
		}
		else if (i == endpage)
		{
			//处理尾部
			willWriteLen = dataLen - (pContext->pagesize - pagepos);
			if (willWriteLen > 0)
			{
				willWriteLen = willWriteLen % pContext->pagesize;
			}
			else
			{
				willWriteLen = 0;
			}

			if (willWriteLen != pContext->pagesize)
			{
				//不是整页
				GetPagePath(pContext, theMeta.dataPage.pPageArray[i], utf8path);
				FILE* pfPage = fopen(utf8path, "rb");
				if (pfPage)
				{
					uint64_t pagesize = pContext->pagesize;
					ReadFileContentWithPos(pfPage, pageBuffer, &pagesize,sizeof(uint64_t));
					fclose(pfPage);
				}
				else
				{
					printf("read page error\n");
					free(pageBuffer);
					return -1;
				}

				memcpy(pageBuffer, pData + dataLen - willWriteLen, willWriteLen);
				willWritePageBuffer = pageBuffer;
			}
			else
			{
				willWritePageBuffer = pData + (pContext->pagesize - pagepos) + (i - startpage - 1) * pContext->pagesize;
				willWriteLen = pContext->pagesize;
			}
		}
		else
		{
			willWritePageBuffer = pData + (pContext->pagesize - pagepos) + (i - startpage - 1) * pContext->pagesize;
			willWriteLen = pContext->pagesize;
		}

		//把准备好的pagebuffer写入新的页面文件
		uint64_t newPageIndex = 0;
		AllocNewPage(pContext, utf8path, &newPageIndex);
		FILE* pfPage = fopen(utf8path, "wb");
		if (pfPage)
		{
			fwrite(&objid, sizeof(uint64_t), 1, pfPage);
			WriteFileContent(pfPage, willWritePageBuffer, willWriteLen);
			theMeta.dataPage.pPageArray[i] = newPageIndex;
			fclose(pfPage);
		}
		else
		{
			printf("write page error\n");
			free(pageBuffer);
			return -1;
		}

	}
	free(pageBuffer);


	uint8_t utf8Path[257] = { 0 };
	uint8_t utf8MetaPath[257] = { 0 };
	GetNewObjPageListDataPath(pContext, utf8Path);
	FILE* pfPageList = fopen(utf8Path, "wb");
	if (pfPageList)
	{
		int bufferSize = GetSizeOfPageList(&(theMeta.dataPage));
		uint8_t *pBuffer = (uint8_t*)malloc(bufferSize);
		WritePageList(pContext, pBuffer, &(theMeta.dataPage));
		WriteFileContent(pfPageList, pBuffer, bufferSize);
		fclose(pfPageList);
		free(pBuffer);

		GetObjMetaPath(pContext, objid, utf8MetaPath);
		FILE* pfMeta = fopen(utf8MetaPath, "wb");
		if (pfMeta) {
			bufferSize = GetSizeOfObjMeta(pContext, &theMeta, utf8Path);
			pBuffer = (uint8_t*)malloc(bufferSize);
			WriteObjMeta(pContext, pBuffer, &theMeta, utf8Path);
			WriteFileContent(pfMeta, pBuffer, bufferSize);
			fclose(pfMeta);
			free(pBuffer);
			free(theMeta.dataPage.pPageArray);
			return 0;
		}
		else
		{
			printf("write meta error\n");
			free(theMeta.dataPage.pPageArray);
			return -1;
		}
	}
	else
	{
		printf("write pagelist error\n");
		free(theMeta.dataPage.pPageArray);
		return -1;
	}

	return -1;
}

int RemoveObj(ObjStoreContext* pContext, uint64_t objid)
{
	uint8_t utf8Path[257] = { 0 };
	GetObjMetaPath(pContext, objid, utf8Path);
	return remove(utf8Path);
}


//---------------------------------------------------------------------------------------
//下面是测试代码
//

void testWrite(ObjStoreContext* pContext, uint16_t maxid, uint16_t mask, size_t datasize)
{
	uint32_t* pdata = (uint32_t*)malloc(datasize);
	for (int i = 0; i < maxid; ++i)
	{
		for (uint32_t j = 0; j < datasize / 4; ++j)
		{
			pdata[j] = ((uint32_t)mask) << 16 | i;
		}

		UpdateObjData(pContext, i, pdata, datasize);
		printf("obj %d update ok.\n", i);
	}
	free(pdata);
}

void testRead(ObjStoreContext* pContext, uint16_t maxid, uint16_t mask, size_t datasize)
{
	uint32_t* pdata = (uint32_t*)malloc(datasize+16);//作业，这是一次性能相关的权衡->解决方案：ReadFileContent要加一个带seek的实现
	for (int i = 0; i < maxid; ++i)
	{
		uint64_t readlen = datasize;
		if (ReadObjData(pContext, i, pdata, &readlen) != 0)
		{
			printf("obj %d test failed.readlen error\n", i);
			continue;
		}

		if (readlen == datasize)
		{
			int testok = 1;
			for (uint32_t j = 0; j < datasize / 4; ++j)
			{
				if (pdata[j] != (((uint32_t)mask << 16) | i))
				{
					testok = 0;
					printf("obj %d test failed. %d byte error. %x\n", i, j, pdata[j]);
					break;
				}
			}
			if (testok)
			{
				printf("obj %d test ok.\n", i);
			}
		}
	}
	free(pdata);
}


void testWriteRange(ObjStoreContext* pContext, uint16_t maxid, uint16_t mask,int startpos, int len)
{
	uint32_t* pdata = (uint32_t*)malloc(len);
	for (int i = 0; i < maxid; ++i)
	{
		for (uint32_t j = 0; j < len / 4; ++j)
		{
			pdata[j] = ((uint32_t)mask) << 16 | i;
		}

		UpdateObjDataByRange(pContext, i, startpos,pdata, len);
		printf("obj %d update range ok.[%d,%d]\n", i,startpos,len);
	}
	free(pdata);
}

void testReadRange(ObjStoreContext* pContext, uint16_t maxid, uint16_t mask, int startpos, int len)
{
	uint32_t* pdata = (uint32_t*)malloc(len + 16);//作业，这是一次性能相关的权衡->解决方案：ReadFileContent要加一个带seek的实现
	for (int i = 0; i < maxid; ++i)
	{
		uint64_t readlen = len;
		if (ReadObjDataByRange(pContext, i,startpos, pdata, &readlen) != 0)
		{
			printf("obj %d read test failed.readlen error\n", i);
			continue;
		}

		if (readlen == len)
		{
			int testok = 1;
			for (uint32_t j = 0; j < len / 4; ++j)
			{
				if (pdata[j] != (((uint32_t)mask << 16) | i))
				{
					testok = 0;
					printf("obj %d test failed. %d byte error. %x\n", i, j*4, pdata[j]);
					break;
				}
			}
			if (testok)
			{
				printf("obj %d test read range ok.[%d,%d]\n", i,startpos,len);
			}
		}
	}
	free(pdata);
}


void testCheck(ObjStoreContext* pContext, uint16_t maxid, size_t datasize)
{
	uint32_t* pdata = (uint32_t*)malloc(datasize+16);//作业，这是一次性能相关的权衡->解决方案：ReadFileContent要加一个带seek的实现
	for (int i = 0; i < maxid; ++i)
	{
		uint64_t readlen = datasize;
		if (ReadObjData(pContext, i, pdata, &readlen) != 0)
		{
			printf("obj %d test failed.readlen error\n", i);
			continue;
		}

		if (readlen == datasize)
		{
			for (uint32_t j = 0; j < datasize / 4; ++j)
			{
				if (j > 0)
				{
					if (pdata[j] != pdata[j - 1])
					{
						printf("obj %d check failed. %d byte error. %x\n", i, j, pdata[j]);
						break;
					}
				}
			}
			printf("obj %d check ok.\n", i);
		}
	}
	free(pdata);
}


int devtest()
{
	ObjStoreContext* pContext = (ObjStoreContext*)malloc(sizeof(ObjStoreContext));
	InitObjStore(pContext, "/tmp/objstore/");
	uint8_t buffer[256];
	uint64_t bufferlen = 256;
	UpdateObjData(pContext, 100, "123456", 6);
	ReadObjData(pContext, 100, buffer, &bufferlen);
	printf("%s\n", buffer);
	if (IsObjExist(pContext, 100) == 0)
	{
		printf("exist\n");
	}
	//RemoveObj(pContext,100);
	if (IsObjExist(pContext, 100) == 0)
	{
		printf("exist\n");
	}

	return 0;
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
	InitObjStore(pContext, "/tmp/objstore/");

	if (argc == 1)
	{
		//for debug
		testWrite(pContext, 10, 0x5f, 9097);
		testRead(pContext, 10, 0x5f, 9097);
		testCheck(pContext, 10, 9097);

		testWriteRange(pContext, 10, 0x6f, 4096, 4096);
		testReadRange(pContext, 10, 0x6f, 4096, 4096);
	}
	else
	{
		if (argc == 5)
		{

			int maxobjid = atoi(argv[2]);
			int mask = atoi(argv[3]);
			int datasize = atoi(argv[4]);

			if(strcmp(argv[1],"-w") == 0)
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