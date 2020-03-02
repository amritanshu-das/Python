import json
import redis
import pymongo
from pymongo import ReplaceOne
from pymongo.errors import BulkWriteError


class SKUFeedProcessor:

    def __init__(self):
        redisConnObj = redis.Redis(host='127.0.0.2', port=6379, db=0)
        self.redisConnObj = redisConnObj
        mongoClient = pymongo.MongoClient('mongodb://localhost:27017/')
        catalogDB = mongoClient['catalog_db']
        self.productCol = catalogDB['products']

    def processJSONFile(self, filePath):
        file = open(filePath, 'r')
        try:
            jsonObj = json.loads(file.read())
        except:
            print('json.loads() failed')
        finally:
            file.close()
        return jsonObj

    def processCategoryFeed(self, categoryJSONObj):
        pim_categories = categoryJSONObj['pim_categories']['category']
        for pim_category in pim_categories:
            parentCategoryId = pim_category['idparentcategory']
            if parentCategoryId != '' or parentCategoryId != 'MainStruct':
                categoryId = pim_category['idcategory'].replace(' ', '_')
                self.redisConnObj.set(categoryId, json.dumps(pim_category))

    def processProductFeed(self, productJSONObj):
        pim_products = productJSONObj['pim_products']
        for pim_product in pim_products:
            productItem = {}
            if len(pim_product['product']['categories']) > 0:
                categoryId = pim_product['product']['categories'][0]['structureGroupNode'].replace(
                    ' ', '_')
                pim_product['product'].pop('categories')
                categoryList = []
                while categoryId != 'Root_Node':
                    categoryNode = json.loads(
                        self.redisConnObj.get(categoryId))
                    categoryList.append(categoryNode)
                    categoryId = categoryNode['idparentcategory'].replace(
                        ' ', '_')
                if len(categoryList) > 0:
                    productItem.update({'categories': categoryList})

                productId = pim_product['product']['idproduct']
                productItem.update({'productId': productId})
                productItem.update({'title': pim_product['product']['title']})
                productItem.update(
                    {'productType': pim_product['product']['productType']})
                productItem.update(
                    {'UPSshippable': pim_product['product']['UPSshippable']})

                if 'stateRestriction' in pim_product['product']:
                    stateRestriction = pim_product['product']['stateRestriction']
                    stateRestrictionList = stateRestriction.split(';')
                    productItem.update(
                        {'restrictedStates': stateRestrictionList})

                # certificationRequiredToBuy
                # statesRequiringCert
                self.redisConnObj.set(productId, json.dumps(productItem))

    def processSKUJSONObj(self, jsonObj):
        pim_skus = jsonObj['pim_skus']
        writeRequests = []

        for pim_sku in pim_skus:
            skuItem = {}
            sku = pim_sku['sku']
            if 'primaryAttributes' in sku:
                skuId = sku['primaryAttributes']['idSku']
                skuItem.update({'_id': skuId})
                productId = sku['primaryAttributes']['idproduct']
                productItem = json.loads(self.redisConnObj.get(productId))
                skuItem.update({'product': productItem})

                skuItem.update(
                    {'itemName': sku['primaryAttributes']['itemName']})
                skuItem.update({'brand': sku['primaryAttributes']['brand']})
                skuItem.update(
                    {'marketingDescription': sku['primaryAttributes']['MarketingDescription']})

                if 'bulletDescription' in sku['primaryAttributes'] and len(sku['primaryAttributes']['bulletDescription']) > 0:
                    features = []
                    bullets = sku['primaryAttributes']['bulletDescription']
                    for bullet in bullets:
                        features.append(bullet['bullet'].strip())
                    skuItem.update({'features': features})

                if 'standardAttributes' in sku:
                    skuItem.update({'upc': sku['standardAttributes']['upc']})
                    skuItem.update(
                        {'catalogNumber': sku['standardAttributes']['CatalogNumber']})
                    skuItem.update(
                        {'wiseItemNo': sku['standardAttributes']['idwin']})

                if 'dynamicAttributes' in sku:
                    dynamicAttrDict = {}
                    for dynamicAttribute in sku['dynamicAttributes']:
                        dynamicAttrDict.update(
                            {dynamicAttribute['name']: dynamicAttribute['value']})
                    skuItem.update({'dynamicAttributes': dynamicAttrDict})

                if 'SearchButNoDisplay' in sku:
                    lc2Sku = sku['SearchButNoDisplay']['LC2sku']
                    availableLCs = lc2Sku.split(';')
                    skuItem.update({'availableLCs': availableLCs})

                if 'images' in sku:
                    images = {'imgLarge': sku['images']['img_large'], 'imgMedium': sku['images']['img_medium'],
                              'imgSmall': sku['images']['img_small']}
                    skuItem.update({'images': images})

                if 'documents' in sku and 'media_cut' in sku['documents']:
                    documents = {'mediaCut': sku['documents']['media_cut']}
                    skuItem.update({'documents': documents})

                if 'packLevels' in sku and len(sku['packLevels']) > 0:
                    b2cPackEcomEligibility = 'No'
                    for packLevel in sku['packLevels']:
                        b2cPackEcomEligibility = packLevel['packLevel']['B2CPackEcomEligibility']
                        if b2cPackEcomEligibility == 'Yes':
                            break
                    skuItem.update(
                        {'b2cPackEcomEligibility': b2cPackEcomEligibility})

                writeRequests.append(ReplaceOne(
                    {'_id': skuId}, skuItem, upsert=True))

        try:
            self.productCol.bulk_write(writeRequests)
        except BulkWriteError as bwe:
            print(bwe.details)


skuFeedProcessor = SKUFeedProcessor()

print('Enter category feed file path :')
catFilePath = input()
catFileJSONObj = skuFeedProcessor.processJSONFile(catFilePath)
skuFeedProcessor.processCategoryFeed(catFileJSONObj)

print('Enter product feed file path :')
prodFilePath = input()
prodFileJSONObj = skuFeedProcessor.processJSONFile(prodFilePath)
skuFeedProcessor.processProductFeed(prodFileJSONObj)

print('Enter sku feed file path :')
skuFilePath = input()
skuFileJSONObj = skuFeedProcessor.processJSONFile(skuFilePath)
skuFeedProcessor.processSKUJSONObj(skuFileJSONObj)
