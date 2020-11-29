import logging
import json
import random
import math
import asyncio
import time
import datetime
import re
from concurrent.futures import ThreadPoolExecutor
from requests import Session
from pyquery import PyQuery as pq
from MegafonAPI import State, BaseConsts, HttpAdapter as MegafonHttpAdapter

class LK:
    name: str
    __metadata: dict
    state: State
    simcards: list

    __session: Session
    __address: str
    __user: str
    __password: str

    __qStats: list

    def __init__(self, address, login, password, name=""):
        self.name = name
        self.state = State()
        self.__address = address
        self.__user = login
        self.__password = password
        self.__session = Session()
        self.__session.mount(f"https://{address}", adapter = MegafonHttpAdapter())
        self.simcards = []
        self.__qStats = {
            "firstQuery": 999999999999,
            "lastQuery": 0,
            "tMinBetweenQueries": 999999999999,
            "count": {}
        }

    def log(self, level, message):
        logging.log(level, f"[LK-{self.name}] {message}")

    def __qStatsAdd(self, fUrl: str):
        t = time.time()

        tMinBetweenQueries = t - self.__qStats["lastQuery"]
        if tMinBetweenQueries < self.__qStats["tMinBetweenQueries"]:
            self.__qStats["tMinBetweenQueries"] = tMinBetweenQueries

        qpOffset = fUrl.find("?")
        url = fUrl[0:qpOffset]

        if not url in self.__qStats["count"]:
            self.__qStats["count"][url] = 0
        self.__qStats["count"][url] += 1

        if t < self.__qStats["firstQuery"]:
            self.__qStats["firstQuery"] = t

        if t > self.__qStats["lastQuery"]:
            self.__qStats["lastQuery"] = t

    def qStatsPrint(self):
        qcs = sum(v for k, v in self.__qStats["count"].items())
        self.log(logging.INFO, f"Performed {qcs} queries in {self.__qStats['lastQuery'] - self.__qStats['firstQuery']} seconds with tMinDiff = {self.__qStats['tMinBetweenQueries']}s")

    def qWait(self):
        t = time.time() - self.__qStats["lastQuery"]
        while t < (1/BaseConsts.QPS.value):
            time.sleep(0.001)
            t = time.time() - self.__qStats["lastQuery"]

    def __performQuery(self, url: str, payload: str, loginQuery = False, method = "POST", contentType = "application/json", parseRosponseJson = True, timeout = BaseConsts.requestTimeout.value):
        self.qWait()
        success = False
        response = None
        responsePayload = None
        loadFailed = False

        r = ""
        for _ in range(6):
            r += str(random.randrange(0, 9))
        r += str(int(time.time()*1000))

        if self.state.loggedin or loginQuery:

            fUrl = url.format(address=self.__address, r=r)
            fData = payload.format(address=self.__address, user=self.__user, password=self.__password)
            headers = {'Content-Type': contentType}
            if "XSRF_TOKEN" in self.__session.cookies:
                headers["x-csrf-token"] = self.__session.cookies["XSRF_TOKEN"]

            self.log(logging.DEBUG, f"[{r}] Performing a request")
            self.log(logging.DEBUG, f" [{r}] METHOD: {method}\n [{r}] URL: {fUrl}\n [{r}] CONTENT-TYPE: '{contentType}'\n [{r}] DATA: {fData}")
            try:
                self.__qStatsAdd(fUrl)
                response = self.__session.request(method=method, url=fUrl, data=fData.encode("utf-8"), headers=headers, timeout=timeout)
                response.encoding = "UTF-8"
                self.log(logging.DEBUG, f"[{r}] Got {response.status_code} status code from server")
            except Exception as e:
                loadFailed = True
                self.log(logging.DEBUG, f"[{r}] Exception occured during server query. {e}")
            
            if not loadFailed:
                try:
                    responsePayload = json.loads(response.text) if parseRosponseJson else response.text
                except Exception as e:
                    self.log(logging.ERROR, f"[{r}] Failed load response JSON: {e}")
                    self.log(logging.DEBUG, f"[{r}]  DATA: {response.text}")
                    loadFailed = True
                finally:
                    response.close()

        if not loginQuery and (loadFailed or not self.state.loggedin or response.status_code != 200 or loadFailed or (parseRosponseJson and "error" in responsePayload and responsePayload["error"])):
            responseCode = response.status_code if response else ""

            self.log(logging.WARNING, f"[{r}] Failed getting response from server")
            self.log(logging.DEBUG, f" [{r}] LOGIN QUERY: {loginQuery}\n [{r}] LOGGED IN: {self.state.loggedin}\n [{r}] STATUS CODE: {responseCode}\n [{r}] LOAD FAILED: {loadFailed}\n [{r}] PAYLOAD: {responsePayload}")
            if not self.state.loggedin or (response and ((response.status_code == 200 and not loadFailed) or (response.status_code == 401) or (response.status_code == 403))):
                if (not self.state.loggedin or response.status_code == 401 or response.status_code == 403 or (parseRosponseJson and (responsePayload["error"] == "NOT_AUTHENTICATED"))) and not loginQuery:
                    self.log(logging.INFO, f"[{r}] Not authenticated. Trying to login")
                    if self.__login():
                        responsePayload = self.__performQuery(url, payload, loginQuery=loginQuery, method=method, parseRosponseJson=parseRosponseJson, contentType=contentType, timeout=timeout)
                        success = True
                    
        else:
            success = True

        return responsePayload if success else None

    def __login(self) -> bool:
        try:
            self.state.loggedin = False
            requestUrl = "https://{address}/ws/v1.0/auth/process"
            requestPayload = "captchaTime=undefined&password={password}&username={user}"
            self.log(logging.INFO, "Loggin into the Megafon LK")

            response = self.__performQuery(requestUrl, requestPayload, loginQuery=True, contentType="application/x-www-form-urlencoded;charset=UTF-8")
            if response and "data" in response and "user" in response["data"] and response["data"]["user"]:
                self.__metadata = response["data"]
                self.state.loggedin =  True
                self.log(logging.INFO, "Successfully logged in to Megafon LK")
            else:
                if response and  "error" in response and "code" in response["error"]:
                    raise Exception(f"Failed to login due to <{response['error']['code']}>")
                else:
                    raise Exception("Got empty response from server")
        except Exception as e:
            self.state.loggedin = False
            self.log(logging.ERROR, f"Unable to login. [{e}]")
        return self.state.loggedin

    def getSimCards(self) -> bool:
        __result = False
        pageSize = 40
        requesListtUrl = "https://{{address}}/ws/v1.0/subscriber/mobile/list?from={start}&size={size}"
        rawcards = []

        def fetchlist_one(page):
            """Fetching one page of card from LK server"""
            self.log(logging.DEBUG, f"Attempting to retrieve the page №{page+1} of connected SIM cards")
            result = None

            for _ in range(10):
                try:
                    result = self.__performQuery(requesListtUrl.format(start=page*pageSize, size=pageSize), "", method="GET")["data"]
                    break
                except Exception as e:
                    result = None
                    self.log(logging.ERROR, f"[{_}] Failed retrieving sim list page №{page}. {e}")
                    time.sleep(_/2)
            if not result:
                raise Exception(f"The attempt to retrieve the page №{page+1} of connected SIM cards failed!")
            
            return result
            
        async def fetchlist_all(pages):
            """Fetching all the page with simcards from LK server asyncronously"""
            with ThreadPoolExecutor(max_workers=BaseConsts.parallelRequests.value) as executor:
                loop = asyncio.get_event_loop()
                asyncio.set_event_loop(loop)
                tasks = [
                    loop.run_in_executor(
                        executor,
                        fetchlist_one,
                        page
                    )
                    for page in range(pages)
                ]
                for response in await asyncio.gather(*tasks):
                    rawcards.extend(response["elements"])

        def fetch_one(sim):
            self.log(logging.DEBUG, f"Attempting to retrieve remains info for simcard №{sim['id']}/{sim['msisdn']}")
            requesInfotUrl = "https://{{address}}/subscriber/info/{simID}"
            try:
                result = self.__performQuery(requesInfotUrl.format(simID=sim["id"]), "", method="GET", parseRosponseJson=False)
                html = pq(result)
                for accountinfo in html("div.account-info__group"):
                    label = pq(accountinfo)("label").text()
                    data = pq(accountinfo)("div.user-status").text()
                    if label == "Статус":
                        sim["raw"]["status"] = data
                    elif label == "Тарифный план":
                        sim["raw"]["ratePlan"] = data
            except Exception as e:
                self.log(logging.WARNING, f"Attempt to retrieve remains info for simcard №{sim['id']}/{sim['msisdn']} failed. {e}")

        async def fetch_all():
            with ThreadPoolExecutor(max_workers=BaseConsts.parallelRequests.value) as executor:
                loop = asyncio.get_event_loop()
                asyncio.set_event_loop(loop)
                tasks = [
                    loop.run_in_executor(
                        executor,
                        fetch_one,
                        sim
                    )
                    for sim in self.simcards
                ]
                await asyncio.gather(*tasks)

        try:
            self.log(logging.INFO, "Getting simcardslist from LK server")

            response = self.__performQuery(requesListtUrl.format(start=0, size=1), "", method="GET")["data"]
            if response:
                self.log(logging.DEBUG, f"There are {response['count']} raw simcards in system. Getting them")

                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                future = asyncio.ensure_future(fetchlist_all(math.ceil(int(response["count"])/pageSize)))
                loop.run_until_complete(future)

                if response["count"] == len(rawcards):
                    self.log(logging.INFO, "Successfully got the simcard list from LK server.")

                    for rawcard in rawcards:
                        existingsim = next(filter(lambda x: x["id"] == rawcard["id"], self.simcards), None)
                        if not existingsim:
                            self.simcards.append({"id": rawcard["id"], "msisdn": rawcard["msisdn"], "raw": rawcard })
                    for sim in self.simcards:
                        rawcard = next(filter(lambda r: r["id"] == sim["id"], rawcards), None)
                        if not rawcard:
                            self.simcards.remove(sim)

                    self.log(logging.INFO, "Getting actual info about every sim from the server")
                    future = asyncio.ensure_future(fetch_all())
                    loop.run_until_complete(future)

                    __result = True

            else:
                raise Exception("Got empty response from server")
        except Exception as e:
            self.log(logging.ERROR, f"Failed. [{e}]")

        return __result

    def getSimServicesInfo(self, simlist) -> bool:
        self.log(logging.INFO, f"Attempting to retrieve services info for {len(simlist)} simcards")

        __result = False

        def services_fetch_one(sim):
            self.log(logging.DEBUG, f"Attempting to retrieve services info for simcard №{sim['id']}/{sim['msisdn']}")
            requesInfotUrl = "https://{{address}}/subscriber/servicesAndRateoptions/{simID}"
            try:
                result = self.__performQuery(requesInfotUrl.format(simID=sim["id"]), "", method="GET", parseRosponseJson=False)
                html = pq(result)
                services = []
                for service in html(".item_category_content div"):
                    label = pq(service)("div").text()
                    services.append(label)
                if len(services) > 0:
                    sim["services"] = services
            except Exception as e:
                self.log(logging.WARNING, f"Attempt to retrieve services info for simcard №{sim['id']}/{sim['msisdn']} failed. {e}")

        async def services_fetch_all(simlist):
            with ThreadPoolExecutor(max_workers=BaseConsts.parallelRequests.value) as executor:
                loop = asyncio.get_event_loop()
                tasks = [
                    loop.run_in_executor(
                        executor,
                        services_fetch_one,
                        sim
                    )
                    for sim in simlist
                ]
                await asyncio.gather(*tasks)

        #-- Services
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        future = asyncio.ensure_future(services_fetch_all(simlist))
        try:
            loop.run_until_complete(future)
            __result = True
            self.log(logging.INFO, "Successfully got simcards services from LK server")
        except Exception as e:
            self.log(logging.WARNING, f"The attempt to retrieve the services info for simcards failed. {e}")
        #-- Services

        return __result

    def __getSimBalanceInfoOld(self, simlist: list):
        """Fetching simcards finance info"""

        __result = False

        self.log(logging.INFO, f"Attempting to retrieve balances info for {len(simlist)} simcards")
        pageSize = 40

        def balance_fetch_one(page):
            __result = False
            """Fetching one page of cards balance from LK server"""
            for _ in range(10):
                try:
                    self.log(logging.DEBUG, f"Attempting to retrieve the page №{page+1} of SIM cards balance")
                    requestUrl = "https://{{address}}/ws/v1.0/expenses/subscriber/from/mobile/list?from={start}&size={size}"
                    response = self.__performQuery(requestUrl.format(start=page*pageSize, size=pageSize), "", method="GET")["data"]
                    if not response:
                        raise Exception(f"The attempt to retrieve the page №{page+1} of SIM cards balance failed!")
                    else:
                        for balanceinfo in response['elements']:
                            for sim in filter(lambda x: x["id"] == str(balanceinfo["subscriberId"]), self.simcards):
                                if not "finance" in sim:
                                    sim["finance"] = {}
                                sim["finance"]["balance"] = { "lastupdated": time.time(), "data": balanceinfo }
                                self.log(logging.DEBUG, f"Successfully retrieved page №{page} of SIM card balances")
                        __result = True
                        break
                except Exception as e:
                    self.log(logging.ERROR, f"[{_}] Failed retrieving the page №{page} of SIM card balance. {e}")
                    time.sleep(_/2)
                
            return __result

        async def balance_fetch_all(pages):
            """Fetching all the page with simcards from LK server asyncronously"""
            with ThreadPoolExecutor(max_workers=BaseConsts.parallelRequests.value) as executor:
                loop = asyncio.get_event_loop()
                asyncio.set_event_loop(loop)
                tasks = [
                    loop.run_in_executor(
                        executor,
                        balance_fetch_one,
                        page
                    )
                    for page in range(pages)
                ]
                await asyncio.gather(*tasks)

        #-- Balance
        try:
            self.log(logging.INFO, "Getting simcards balance list from LK server")

            # # Cleaning basket //POST
            requestUrl = "https://b2blk.megafon.ru/ws/v1.0/subscriber/mobile/basket/delete"
            requestPayload = "{{}}"
            response = self.__performQuery(requestUrl, requestPayload)

            # Adding sims to basket // POST
            requestUrl = "https://{address}/ws/v1.0/subscriber/mobile/basket/add"
            requestPayloadItems = [{"id": int(sim["id"]), "label": sim["msisdn"], "value": True} for sim in simlist]
            items = json.dumps(requestPayloadItems, ensure_ascii=False).replace('{','{{').replace('}','}}')
            requestPayload = f'{{{{"items":{items}}}}}'
            response = self.__performQuery(requestUrl, requestPayload)

            # Getting balance info // GET
            requestUrl = "https://{{address}}/ws/v1.0/expenses/subscriber/from/mobile/list?from={start}&size={size}"
            response = self.__performQuery(requestUrl.format(start=0, size=1), "", method="GET")["data"]
            if response:
                self.log(logging.DEBUG, f"There are {response['count']} raw simcard balance info in the system. Getting them")

                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                future = asyncio.ensure_future(balance_fetch_all(math.ceil(int(response["count"])/pageSize)))
                loop.run_until_complete(future)

                __result = True
                self.log(logging.INFO, "Successfully got simcards balance list from LK server")

            else:
                raise Exception("Got empty response from server")
        except Exception as e:
            self.log(logging.ERROR, f"Failed. [{e}]")
        #-- Balance

        return __result

    def getSimBalanceInfo(self, simlist) -> bool:
        self.log(logging.INFO, f"Attempting to retrieve balance info for {len(simlist)} simcards")

        __result = False

        def balances_fetch_one(sim):
            self.log(logging.DEBUG, f"Attempting to retrieve balance info for simcard №{sim['id']}/{sim['msisdn']}")
            requesInfotUrl = "https://{{address}}/subscriber/finances/{simID}"
            try:
                result = self.__performQuery(requesInfotUrl.format(simID=sim["id"]), "", method="GET", parseRosponseJson=False)
                html = pq(result)
                balanceinfo = {
                    'amountTotal': 0.0,
                    'monthChargeRTPL': 0.0,
                    'amountCountryRoaming': 0.0,
                    'monthChargeSRLS': 0.0,
                    'charges': 0.0,
                    'amountLocal': 0.0,
                    'amountRoumnig': 0.0,
                    'amountLocalMacro': 0.0
                }
                amountTotal = 0

                if html:
                    for amountRow in html('.span50'):

                        title = html(amountRow).prevAll()[0].text

                        amountText = html(amountRow).find(".money").text()
                        amountText = re.sub("[^0-9,]", "", amountText).replace(",", ".")
                        amount = float(amountText)

                        if title == 'Расходы с начала периода':
                            amountTotal = amount
                        elif title == "Начисление":
                            balanceinfo['monthChargeRTPL'] = amount
                            balanceinfo['amountTotal'] += amount
                        elif title == "Блокировка корпоративных клиентов":
                            balanceinfo['monthChargeSRLS'] += amount
                            balanceinfo['amountTotal'] += amount
                        elif title == "Прочие услуги в домашнем регионе":
                            balanceinfo['amountLocal'] += amount
                            balanceinfo['amountTotal'] += amount
                        elif title == "По тарифному плану":
                            pass
                        elif title == "Баланс персонального счета":
                            pass
                        else:
                            self.log(logging.WARNING, f"Attempt to retrieve balance info for simcard №{sim['id']}/{sim['msisdn']}. Unknown balance title: '{title}'")
                    else:
                        if amountTotal > balanceinfo['amountTotal']:
                            balanceinfo['monthChargeRTPL'] += (amountTotal - balanceinfo['amountTotal'])
                            balanceinfo['amountTotal'] = amountTotal
                        if "finance" not in sim:
                            sim["finance"] = {}
                        sim["finance"]["balance"] = { "lastupdated": time.time(), "data": balanceinfo }
            except Exception as e:
                self.log(logging.WARNING, f"Attempt to retrieve balance info for simcard №{sim['id']}/{sim['msisdn']} failed. {e}")

        async def balances_fetch_all(simlist):
            with ThreadPoolExecutor(max_workers=BaseConsts.parallelRequests.value) as executor:
                loop = asyncio.get_event_loop()
                tasks = [
                    loop.run_in_executor(
                        executor,
                        balances_fetch_one,
                        sim
                    )
                    for sim in simlist
                ]
                await asyncio.gather(*tasks)

        #-- Balance
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        future = asyncio.ensure_future(balances_fetch_all(simlist))
        try:
            loop.run_until_complete(future)
            __result = True
            self.log(logging.INFO, "Successfully got simcards balance from LK server")
        except Exception as e:
            self.log(logging.WARNING, f"The attempt to retrieve the balance info for simcards failed. {e}")
        #-- Balance

        return __result

    def getSimRemainsInfo(self, simlist: list):
        """Fetching simcards finance info"""

        self.log(logging.INFO, f"Attempting to retrieve remains info for {len(simlist)} simcards")

        __result = False

        def remains_fetch_one(sim):
            __result = False
            self.log(logging.DEBUG, f"Attempting to retrieve remains info for simcard №{sim['id']}/{sim['msisdn']}")
            discounts: list = None
            for _ in range(10):
                try:
                    requestUrl = "https://{{address}}/subscriber/info/{simID}/discounts?_={{r}}"
                    resultMinutes = self.__performQuery(requestUrl.format(simID=sim["id"]), "", method="GET")
                    if resultMinutes:
                        discounts = resultMinutes["discounts"]["oapiDiscounts"]
                    else:
                        raise Exception("Empty response?")
                    break
                except Exception as e:
                    self.log(logging.ERROR, f"[{attempt}] Failed retrieving remaining minutes info for simcard №{sim['id']}/{sim['msisdn']}. {e}")
                    time.sleep(_/2)

            if discounts != None:
                if not "finance" in sim:
                    sim["finance"] = {}
                sim["finance"]["discounts"] = { "lastupdated": time.time(), "data": discounts }
                __result = True
                self.log(logging.DEBUG, f"fSuccessfully retrieved remains info for simcard №{sim['id']}/{sim['msisdn']}.")
            else:
                self.log(logging.ERROR, f"Failed retrieving remains info info for simcard №{sim['id']}/{sim['msisdn']}.")

            return __result

        async def remains_fetch_all(simlist):
            with ThreadPoolExecutor(max_workers=BaseConsts.parallelRequests.value) as executor:
                loop = asyncio.get_event_loop()
                tasks = [
                    loop.run_in_executor(
                        executor,
                        remains_fetch_one,
                        sim
                    )
                    for sim in simlist
                ]
                await asyncio.gather(*tasks)

        #-- Remains
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        #future = asyncio.ensure_future(remains_fetch_all(list(filter(lambda sim: sim["raw"]["status"] == "Активен", simlist))))
        future = asyncio.ensure_future(remains_fetch_all(simlist))
        try:
            loop.run_until_complete(future)
            __result = True
            self.log(logging.INFO, "Successfully got simcards remains from LK server")
        except Exception as e:
            self.log(logging.WARNING, f"The attempt to retrieve the finance info for simcards failed. {e}")
        #-- Remains

        return __result

    def getSimDCRulesInfo(self, simlist: list):
        """Fetching simcards finance info"""

        self.log(logging.INFO, f"Attempting to retrieve dcrules info for {len(simlist)} simcards")

        __result = False

        def dcrules_fetch_one(sim):
            __result = False
            self.log(logging.DEBUG, f"Attempting to retrieve dcrules info for simcard №{sim['id']}/{sim['msisdn']}")
            dcrules = None
            for _ in range(10):
                try:
                    requestUrl = "https://{{address}}/subscriber/dcrule/{simID}/dcrules?_={{r}}"
                    resultRules = self.__performQuery(requestUrl.format(simID=sim["id"]), "", method="GET")
                    if resultRules:
                        dcrules = resultRules['list']
                        for dcrule in dcrules:
                            requestUrl = "https://{{address}}/subscriber/dcrule/{simID}/dcrules/{ruleID}?_={{r}}"
                            resultRuleDetail = self.__performQuery(requestUrl.format(simID=sim["id"], ruleID=dcrule["subscriberDistributeChargesRuleSetId"]), "", method="GET")
                            if resultRuleDetail:
                                dcrule["detail"] = resultRuleDetail['list']
                            else:
                                dcrules = None
                                raise Exception("Empty response?")
                        else:
                            break
                    else:
                        dcrules = None
                        raise Exception("Empty response?")

                except Exception as e:
                    self.log(logging.ERROR, f"[{attempt}] Failed retrieving dcrules for simcard №{sim['id']}/{sim['msisdn']}. {e}")
                    time.sleep(_/2)

            if dcrules != None:
                if not "finance" in sim:
                    sim["finance"] = {}
                sim["finance"]["dcrules"] = { "lastupdated": time.time(), "data": dcrules }
                __result = True
                self.log(logging.DEBUG, f"Successfully retrieved dcrules info for simcard №{sim['id']}/{sim['msisdn']}.")
            else:
                self.log(logging.ERROR, f"Failed retrieving dcrules info info for simcard №{sim['id']}/{sim['msisdn']}.")

            return __result

        async def dcrules_fetch_all(simlist):
            with ThreadPoolExecutor(max_workers=BaseConsts.parallelRequests.value) as executor:
                loop = asyncio.get_event_loop()
                tasks = [
                    loop.run_in_executor(
                        executor,
                        dcrules_fetch_one,
                        sim
                    )
                    for sim in simlist
                ]
                await asyncio.gather(*tasks)

        #-- dcrules
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        #future = asyncio.ensure_future(remains_fetch_all(list(filter(lambda sim: sim["raw"]["status"] == "Активен", simlist))))
        future = asyncio.ensure_future(dcrules_fetch_all(simlist))
        try:
            loop.run_until_complete(future)
            __result = True
            self.log(logging.INFO, "Successfully got simcards dcrules from LK server")
        except Exception as e:
            self.log(logging.WARNING, f"The attempt to retrieve the finance dcrules for simcards failed. {e}")
        #-- dcrules

        return __result

    def resetSimDcRule(self, simlist: list):
        __result = False

        requestUrl = "https://{address}/changedcrules/order"
        simcards = ','.join([_["id"] for _ in simlist])
        date = datetime.datetime.utcnow().strftime('%d.%m.%Y')
        requestPayload = f'&p_basket=-1&notifySubsBySms=&operationType=0&dateFrom={date}&ruleCommon=true&ruleSets=[{{{{"limits":[],"customerRuleSet":{{{{}}}},"commonRuleSet":{{{{}}}}}}}}]&cpohId=0&processingMode=1&subsId={simcards}'
        response = self.__performQuery(requestUrl, requestPayload, contentType="application/x-www-form-urlencoded;charset=UTF-8")

        if response:
            self.log(logging.INFO, "The order for clearing simcard's limits has been successfully created")
            __result = True

        return __result

    def setSimDcRule(self, simlist: list, limit: int):
        __result = False

        requestUrl = "https://{address}/changedcrules/order"
        simcards = ','.join([_["id"] for _ in simlist])
        date = datetime.datetime.utcnow().strftime('%d.%m.%Y')
        requestPayload = f"&p_basket=-1&notifySubsBySms=&operationType=1&dateFrom={date}&ruleCommon=true&ruleSets=[{{{{\"subscriberRuleSetId\":\"823\",\"ruleSetId\":\"823\",\"limits\":[{{{{\"ruleId\":\"847\",\"name\":\"АП по тарифу\",\"warnLimit\":\"5\",\"breakLimit\":\"{limit}\",\"limitMeasureUnit\":{{{{\"name\":\"Внутренняя валюта\",\"limitMeasureUnitId\":3,\"measureType\":2}}}}}}}}],\"commonRuleSet\":{{{{\"distributeChargesRuleCommonSetId\":823,\"name\":\"АП по тарифу\",\"status\":{{{{\"distributeChargesRuleCommonSetStatusId\":3,\"name\":\"Используется\"}}}},\"allowUse\":true,\"deleteDate\":null,\"deleteUser\":null,\"createDate\":1530822026000,\"createUser\":\"ROMAN_FEDOSEEV\",\"changeDate\":1530822026000,\"changeUser\":\"ROMAN_FEDOSEEV\",\"commonLimit\":{{{{\"distributeChargesRuleCode\":847,\"name\":\"Корпоративный счет, в рамках лимита\",\"applyToCharges\":[{{{{\"type\":\"OTHER_CHARGES\",\"name\":\"Прочие виды начислений\"}}}}],\"limit\":{{{{\"measureUnit\":{{{{\"limitMeasureUnitId\":3,\"name\":\"Внутренняя валюта\",\"abbreviation\":\"руб.\",\"measureType\":{{{{\"measureTypeId\":2,\"name\":\"Деньги\"}}}}}}}},\"break\":20,\"warning\":15}}}},\"childRules\":[]}}}},\"corporateRule\":{{{{\"distributeChargesRuleCode\":848,\"name\":\"Корпоративный счет\",\"applyToCharges\":[{{{{\"type\":\"CALL_CHARGES\",\"profile\":{{{{\"ratingProfileId\":305,\"name\":\"Абонентская плата (профиль АП по тарифу)\",\"measureType\":{{{{\"measureTypeId\":4,\"name\":\"Календарные единицы\"}}}}}}}}}}}},{{{{\"type\":\"PRODUCT_CHARGES\",\"profile\":{{{{\"productProfileId\":401,\"name\":\"Профиль \\\"АП по тарифу\\\"\"}}}}}}}}],\"childRules\":[]}}}},\"personalRule\":{{{{\"distributeChargesRuleCode\":849,\"name\":\"Персональный счет\",\"applyToCharges\":[],\"childRules\":[]}}}}}}}}}}}}]&cpohId=0&processingMode=1&subsId={simcards}"
        response = self.__performQuery(requestUrl, requestPayload, contentType="application/x-www-form-urlencoded;charset=UTF-8")

        if response:
            self.log(logging.INFO, "The order for setting simcard's limits has been successfully created")
            self.log(logging.INFO, "DC rules successfully set")
            __result = True

        return __result

    def disableSim(self, simlist: list):
        __result = False

        requestUrl = "https://{address}/changelock/order"
        simcards = ','.join([_["id"] for _ in simlist])
        dateFrom = datetime.datetime.now().strftime('%d.%m.%Y %H:%M')
        dateTo = (datetime.datetime.now()+datetime.timedelta(days=365)).strftime('%d.%m.%Y')
        requestPayload = f'&p_basket=-1&notifySubsBySms=&operationType=1&dateFrom={dateFrom}:00&dateTo={dateTo} 00:00:00&cpohId=0&processingMode=1&subsId={simcards}'
        response = self.__performQuery(requestUrl, requestPayload, contentType="application/x-www-form-urlencoded;charset=UTF-8")

        if response:
            self.log(logging.INFO, "The order for disabling simcards has been successfully created")
            __result = True
        return __result

    def enableSim(self, simlist: list):
        __result = False

        requestUrl = "https://{address}/changelock/order"
        simcards = ','.join([_["id"] for _ in simlist])
        date = datetime.datetime.now().strftime('%d.%m.%Y %H:%M')
        requestPayload = f'&p_basket=-1&notifySubsBySms=&operationType=0&dateFrom={date}:00&cpohId=0&processingMode=1&subsId={simcards}'
        response = self.__performQuery(requestUrl, requestPayload, contentType="application/x-www-form-urlencoded;charset=UTF-8")

        if response:
            self.log(logging.INFO, "The order for disabling simcards has been successfully created")
            __result = True

        return __result

    def setSimPlan(self, simlist: list, plan: str):
        """
            Replaces active plan for listed simcards with the given by plan id
            e.g. 3382 - "Интернет Вещей"
                 1770 - "Интернет вещей 2018"
        """

        __result = False

        requestUrl = "https://{address}/changepkgrateplan/order"
        simcards = ','.join([_["id"] for _ in simlist])
        date = datetime.datetime.utcnow().strftime('%d.%m.%Y')
        requestPayload = f'&p_basket=-1&packList=&cpohId=0&processingMode=1&p_excludeSubs=&rtplId={plan}&dateFrom={date}&subsId={simcards}'
        response = self.__performQuery(requestUrl, requestPayload, contentType="application/x-www-form-urlencoded;charset=UTF-8")

        if response and "changeResult" in response and response["changeResult"]["cporId"] > 0:
            self.log(logging.INFO, "The order for simcards' plan change has been successfully created")
            __result = True

        return __result

