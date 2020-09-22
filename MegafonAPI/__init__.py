"""Megafon API"""
import logging
import string
import json
import random
import math
import asyncio
import time
import datetime
import pytz
import re
from concurrent.futures import ThreadPoolExecutor
from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.poolmanager import PoolManager
from pyquery import PyQuery as pq

requestTimeout = 30
parallelRequests = 50
QPS = 40

class MegafonHttpAdapter(HTTPAdapter):
    def init_poolmanager(self, connections, maxsize, block=False):
        self.poolmanager = PoolManager(num_pools=10, maxsize=200, block=block)

class SimCard:
    phoneNumber: str
    lkEnabled: bool
    lkExtendedStatus: str
    lkTariff: str
    lkBalance: float
    lkMinutesRemain: int
    vatsConected: bool
    vatsEnabed: bool
    vatsType: str
    vatsUser: str
    vatsLogin: str
    vatsPassord: str

    def __init__(self):
        self.phoneNumber = ""
        self.lkEnabled = False
        self.lkExtendedStatus = ""
        self.lkTariff = ""
        self.lkBalance = 0
        self.lkMinutesRemain = 0
        self.vatsConected = False
        self.vatsEnabed = False
        self.vatsType = ""
        self.vatsUser = ""
        self.vatsLogin = ""
        self.vatsPassord = ""

class State:
    loggedin: bool

    def __init__(self):
        self.loggedin = False

class MegafonAPILK:
    name: str
    __metadata: dict
    state: State
    simcards: list

    __session: Session
    __address: string
    __user: string
    __password: string

    __qStats: list

    def __init__(self, address, login, password, name=""):
        self.name = name
        self.state = State()
        self.__address = address
        self.__user = login
        self.__password = password
        self.__session = Session()
        self.__session.mount('https://{address}'.format(address=address), adapter = MegafonHttpAdapter())
        self.simcards = []
        self.__qStats = {
            "firstQuery": 999999999999,
            "lastQuery": 0,
            "tMinBetweenQueries": 999999999999,
            "count": {}
        }

    def log(self, level, message):
        logging.log(level, "[LK-{name}] {message}".format(name=self.name, message=message))

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
        self.log(logging.INFO, "Performed {0} queries in {1} seconds with tMinDiff = {2}s".format(qcs, self.__qStats["lastQuery"] - self.__qStats["firstQuery"], self.__qStats["tMinBetweenQueries"]))

    def qWait(self):
        t = time.time() - self.__qStats["lastQuery"]
        while t < (1/QPS):
            time.sleep(0.001)
            t = time.time() - self.__qStats["lastQuery"]

    def __performQuery(self, url: string, payload: string, loginQuery = False, method = "POST", contentType = "application/json", parseRosponseJson = True, timeout = requestTimeout):
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

            self.log(logging.DEBUG, "[{r}] Performing a request".format(r=r))
            self.log(logging.DEBUG, " [{r}] METHOD: {method}\n [{r}] URL: {url}\n [{r}] CONTENT-TYPE: '{contenttype}'\n [{r}] DATA: {payload}".format(method=method, url=fUrl, payload=fData, contenttype=contentType, r=r))
            try:
                self.__qStatsAdd(fUrl)
                response = self.__session.request(method=method, url=fUrl, data=fData.encode("utf-8"), headers=headers, timeout=timeout)
                response.encoding = "UTF-8"
                self.log(logging.DEBUG, "[{r}] Got {code} status code from server".format(code=response.status_code, r=r))
            except Exception as e:
                loadFailed = True
                self.log(logging.DEBUG, "[{r}] Exception occured during server query. {e}".format(r=r, e=e))
            
            if not loadFailed:
                try:
                    responsePayload = json.loads(response.text) if parseRosponseJson else response.text
                except Exception as e:
                    self.log(logging.ERROR, "[{r}] Failed load response JSON: {e}".format(e=e, r=r))
                    self.log(logging.DEBUG, "[{r}]  DATA: {d}".format(d=response.text, r=r))
                    loadFailed = True
                finally:
                    response.close()

        if not loginQuery and (loadFailed or not self.state.loggedin or response.status_code != 200 or loadFailed or (parseRosponseJson and "error" in responsePayload and responsePayload["error"])):
            self.log(logging.WARNING, "[{r}] Failed getting response from server".format(r=r))
            self.log(logging.DEBUG, " [{r}] LOGIN QUERY: {loginquery}\n [{r}] LOGGED IN: {loggedin}\n [{r}] STATUS CODE: {statuscode}\n [{r}] LOAD FAILED: {loadfailed}\n [{r}] PAYLOAD: {payload}". format(
                r=r,
                loginquery=loginQuery,
                loggedin=self.state.loggedin,
                statuscode=response.status_code if response else "",
                loadfailed=loadFailed,
                payload=responsePayload
            ))
            if not self.state.loggedin or (response and ((response.status_code == 200 and not loadFailed) or (response.status_code == 401) or (response.status_code == 403))):
                if (not self.state.loggedin or response.status_code == 401 or response.status_code == 403 or (parseRosponseJson and (responsePayload["error"] == "NOT_AUTHENTICATED"))) and not loginQuery:
                    self.log(logging.INFO, "[{r}] Not authenticated. Trying to login".format(r=r))
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
                    raise Exception("Failed to login due to <{0}>".format(response["error"]["code"]))
                else:
                    raise Exception("Got empty response from server")
        except Exception as e:
            self.state.loggedin = False
            self.log(logging.ERROR, "Unable to login. [{exception}]".format(exception=e))
        return self.state.loggedin

    def getSimCards(self) -> bool:
        __result = False
        pageSize = 40
        requesListtUrl = "https://{{address}}/ws/v1.0/subscriber/mobile/list?from={start}&size={size}"
        rawcards = []

        def fetchlist_one(page):
            """Fetching one page of card from LK server"""
            self.log(logging.DEBUG, "Attempting to retrieve the page №{page} of connected SIM cards".format(page=page+1))
            result = None

            for _ in range(10):
                try:
                    result = self.__performQuery(requesListtUrl.format(start=page*pageSize, size=pageSize), "", method="GET")["data"]
                    break
                except Exception as e:
                    result = None
                    self.log(logging.ERROR, "[{attempt}] Failed retrieving sim list page №{page}. {e}".format(attempt=_,page=page, e=e))
                    time.sleep(_/2)
            if not result:
                raise Exception("The attempt to retrieve the page №{page} of connected SIM cards failed!".format(page=page+1))
            
            return result
            
        async def fetchlist_all(pages):
            """Fetching all the page with simcards from LK server asyncronously"""
            with ThreadPoolExecutor(max_workers=parallelRequests) as executor:
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
            self.log(logging.DEBUG, "Attempting to retrieve remains info for simcard №{simID}/{simPN}".format(simID=sim["id"], simPN=sim["msisdn"]))
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
                self.log(logging.WARNING, "Attempt to retrieve remains info for simcard №{simID}/{simPN} failed. {e}".format(simID=sim["id"], simPN=sim["msisdn"], e=e))

        async def fetch_all():
            with ThreadPoolExecutor(max_workers=parallelRequests) as executor:
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
                self.log(logging.DEBUG, "There are {count} raw simcards in system. Getting them".format(count=response["count"]))

                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                future = asyncio.ensure_future(fetchlist_all(math.ceil(int(response["count"])/pageSize)))
                loop.run_until_complete(future)

                if response["count"] == len(rawcards):
                    self.log(logging.INFO, "Successfully got the simcard list from LK server.")

                    for rawcard in rawcards:
                        existingsim = None
                        for sim in filter(lambda x: x["id"] == rawcard["id"], self.simcards):
                            existingsim = sim
                        
                        if existingsim:
                            existingsim["raw"] = rawcard
                        else:
                            self.simcards.append({"id": rawcard["id"], "msisdn": rawcard["msisdn"], "raw": rawcard })
                    for sim in self.simcards:
                        isabsent = True
                        for rawcard in filter(lambda r: r["id"] == sim["id"], rawcards):
                            isabsent = False
                        if isabsent:
                            self.simcards.remove(sim)

                    self.log(logging.INFO, "Getting actual info about every sim from the server")
                    future = asyncio.ensure_future(fetch_all())
                    loop.run_until_complete(future)

                    __result = True

            else:
                raise Exception("Got empty response from server")
        except Exception as e:
            self.log(logging.ERROR, "Failed. [{exception}]".format(exception=e))

        return __result

    def getSimServicesInfo(self, simlist) -> bool:
        self.log(logging.INFO, "Attempting to retrieve services info for {count} simcards".format(count=len(simlist)))

        __result = False

        def services_fetch_one(sim):
            self.log(logging.DEBUG, "Attempting to retrieve services info for simcard №{simID}/{simPN}".format(simID=sim["id"], simPN=sim["msisdn"]))
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
                self.log(logging.WARNING, "Attempt to retrieve services info for simcard №{simID}/{simPN} failed. {e}".format(simID=sim["id"], simPN=sim["msisdn"], e=e))

        async def services_fetch_all(simlist):
            with ThreadPoolExecutor(max_workers=parallelRequests) as executor:
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
            self.log(logging.WARNING, "The attempt to retrieve the services info for simcards failed. {e}".format(e=e))
        #-- Services

        return __result

    def __getSimBalanceInfoOld(self, simlist: list):
        """Fetching simcards finance info"""

        __result = False

        self.log(logging.INFO, "Attempting to retrieve balances info for {count} simcards".format(count=len(simlist)))
        pageSize = 40

        def balance_fetch_one(page):
            __result = False
            """Fetching one page of cards balance from LK server"""
            for _ in range(10):
                try:
                    self.log(logging.DEBUG, "Attempting to retrieve the page №{page} of SIM cards balance".format(page=page+1))
                    requestUrl = "https://{{address}}/ws/v1.0/expenses/subscriber/from/mobile/list?from={start}&size={size}"
                    response = self.__performQuery(requestUrl.format(start=page*pageSize, size=pageSize), "", method="GET")["data"]
                    if not response:
                        raise Exception("The attempt to retrieve the page №{page} of SIM cards balance failed!".format(page=page+1))
                    else:
                        for balanceinfo in response['elements']:
                            for sim in filter(lambda x: x["id"] == str(balanceinfo["subscriberId"]), self.simcards):
                                if not "finance" in sim:
                                    sim["finance"] = {}
                                sim["finance"]["balance"] = { "lastupdated": time.time(), "data": balanceinfo }
                                self.log(logging.DEBUG, "Successfully retrieved page №{page} of SIM card balances".format(page=page))
                        __result = True
                        break
                except Exception as e:
                    self.log(logging.ERROR, "[{attempt}] Failed retrieving the page №{page} of SIM card balance. {e}".format(attempt=_, page=page, e=e))
                    time.sleep(_/2)
                
            return __result

        async def balance_fetch_all(pages):
            """Fetching all the page with simcards from LK server asyncronously"""
            with ThreadPoolExecutor(max_workers=parallelRequests) as executor:
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
            requestPayload = '{{{{"items":{items}}}}}'.format(items=json.dumps(requestPayloadItems, ensure_ascii=False).replace('{','{{').replace('}','}}'))
            response = self.__performQuery(requestUrl, requestPayload)

            # Getting balance info // GET
            requestUrl = "https://{{address}}/ws/v1.0/expenses/subscriber/from/mobile/list?from={start}&size={size}"
            response = self.__performQuery(requestUrl.format(start=0, size=1), "", method="GET")["data"]
            if response:
                self.log(logging.DEBUG, "There are {count} raw simcard balance info in the system. Getting them".format(count=response["count"]))

                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                future = asyncio.ensure_future(balance_fetch_all(math.ceil(int(response["count"])/pageSize)))
                loop.run_until_complete(future)

                __result = True
                self.log(logging.INFO, "Successfully got simcards balance list from LK server")

            else:
                raise Exception("Got empty response from server")
        except Exception as e:
            self.log(logging.ERROR, "Failed. [{exception}]".format(exception=e))
        #-- Balance

        return __result

    def getSimBalanceInfo(self, simlist) -> bool:
        self.log(logging.INFO, "Attempting to retrieve balance info for {count} simcards".format(count=len(simlist)))

        __result = False

        def balances_fetch_one(sim):
            self.log(logging.DEBUG, "Attempting to retrieve balance info for simcard №{simID}/{simPN}".format(simID=sim["id"], simPN=sim["msisdn"]))
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

                if html:
                    for amountRow in html('.span50'):

                        title = html(amountRow).prevAll()[0].text

                        amountText = html(amountRow).find(".money").text()
                        amountText = re.sub("[^0-9,]", "", amountText).replace(",", ".")
                        amount = float(amountText)

                        if title == 'Расходы с начала периода':
                            pass
                        elif title == "Начисление":
                            balanceinfo['monthChargeRTPL'] = amount
                            balanceinfo['amountTotal'] += amount
                        elif title == "Блокировка корпоративных клиентов":
                            balanceinfo['monthChargeSRLS'] += amount
                            balanceinfo['amountTotal'] += amount
                        elif title == "Прочие услуги в домашнем регионе":
                            balanceinfo['amountLocal'] += amount
                        elif title == "По тарифному плану":
                            pass
                        elif title == "Баланс персонального счета":
                            pass
                        else:
                            self.log(logging.WARNING, "Attempt to retrieve balance info for simcard №{simID}/{simPN}. Unknown balance title: '{title}'".format(simID=sim["id"], simPN=sim["msisdn"], title=title))
                    else:
                        if "finance" not in sim:
                            sim["finance"] = {}
                        sim["finance"]["balance"] = { "lastupdated": time.time(), "data": balanceinfo }
            except Exception as e:
                self.log(logging.WARNING, "Attempt to retrieve balance info for simcard №{simID}/{simPN} failed. {e}".format(simID=sim["id"], simPN=sim["msisdn"], e=e))

        async def balances_fetch_all(simlist):
            with ThreadPoolExecutor(max_workers=parallelRequests) as executor:
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
            self.log(logging.WARNING, "The attempt to retrieve the balance info for simcards failed. {e}".format(e=e))
        #-- Balance

        return __result

    def getSimRemainsInfo(self, simlist: list):
        """Fetching simcards finance info"""

        self.log(logging.INFO, "Attempting to retrieve remains info for {count} simcards".format(count=len(simlist)))

        __result = False

        def remains_fetch_one(sim):
            __result = False
            self.log(logging.DEBUG, "Attempting to retrieve remains info for simcard №{simID}/{simPN}".format(simID=sim["id"], simPN=sim["msisdn"]))
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
                    self.log(logging.ERROR, "[{attempt}] Failed retrieving remaining minutes info for simcard №{simID}/{simPN}. {e}".format(attempt=_,simID=sim["id"], simPN=sim["msisdn"], e=e))
                    time.sleep(_/2)

            if discounts != None:
                if not "finance" in sim:
                    sim["finance"] = {}
                sim["finance"]["discounts"] = { "lastupdated": time.time(), "data": discounts }
                __result = True
                self.log(logging.DEBUG, "Successfully retrieved remains info for simcard №{simID}/{simPN}".format(simID=sim["id"], simPN=sim["msisdn"]))
            else:
                self.log(logging.ERROR, "Failed retrieving remains info info for simcard №{simID}/{simPN}.".format(simID=sim["id"], simPN=sim["msisdn"]))

            return __result

        async def remains_fetch_all(simlist):
            with ThreadPoolExecutor(max_workers=parallelRequests) as executor:
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
            self.log(logging.WARNING, "The attempt to retrieve the finance info for simcards failed. {e}".format(e=e))
        #-- Remains

        return __result

    def getSimDCRulesInfo(self, simlist: list):
        """Fetching simcards finance info"""

        self.log(logging.INFO, "Attempting to retrieve dcrules info for {count} simcards".format(count=len(simlist)))

        __result = False

        def dcrules_fetch_one(sim):
            __result = False
            self.log(logging.DEBUG, "Attempting to retrieve dcrules info for simcard №{simID}/{simPN}".format(simID=sim["id"], simPN=sim["msisdn"]))
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
                    self.log(logging.ERROR, "[{attempt}] Failed retrieving dcrules for simcard №{simID}/{simPN}. {e}".format(attempt=_,simID=sim["id"], simPN=sim["msisdn"], e=e))
                    time.sleep(_/2)

            if dcrules != None:
                if not "finance" in sim:
                    sim["finance"] = {}
                sim["finance"]["dcrules"] = { "lastupdated": time.time(), "data": dcrules }
                __result = True
                self.log(logging.DEBUG, "Successfully retrieved dcrules info for simcard №{simID}/{simPN}".format(simID=sim["id"], simPN=sim["msisdn"]))
            else:
                self.log(logging.ERROR, "Failed retrieving dcrules info info for simcard №{simID}/{simPN}.".format(simID=sim["id"], simPN=sim["msisdn"]))

            return __result

        async def dcrules_fetch_all(simlist):
            with ThreadPoolExecutor(max_workers=parallelRequests) as executor:
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
            self.log(logging.WARNING, "The attempt to retrieve the finance dcrules for simcards failed. {e}".format(e=e))
        #-- dcrules

        return __result

    def resetSimDcRule(self, simlist: list):
        __result = False

        requestUrl = "https://{address}/changedcrules/order"
        simcards = ','.join([_["id"] for _ in simlist])
        date = datetime.datetime.utcnow().strftime('%d.%m.%Y')
        requestPayload = '&p_basket=-1&notifySubsBySms=&operationType=0&dateFrom={date}&ruleCommon=true&ruleSets=[{{{{"limits":[],"customerRuleSet":{{{{}}}},"commonRuleSet":{{{{}}}}}}}}]&cpohId=0&processingMode=1&subsId={simcards}'.format(date=date,simcards=simcards)
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
        requestPayload = "&p_basket=-1&notifySubsBySms=&operationType=1&dateFrom={date}&ruleCommon=true&ruleSets=[{{{{\"subscriberRuleSetId\":\"823\",\"ruleSetId\":\"823\",\"limits\":[{{{{\"ruleId\":\"847\",\"name\":\"АП по тарифу\",\"warnLimit\":\"5\",\"breakLimit\":\"{limit}\",\"limitMeasureUnit\":{{{{\"name\":\"Внутренняя валюта\",\"limitMeasureUnitId\":3,\"measureType\":2}}}}}}}}],\"commonRuleSet\":{{{{\"distributeChargesRuleCommonSetId\":823,\"name\":\"АП по тарифу\",\"status\":{{{{\"distributeChargesRuleCommonSetStatusId\":3,\"name\":\"Используется\"}}}},\"allowUse\":true,\"deleteDate\":null,\"deleteUser\":null,\"createDate\":1530822026000,\"createUser\":\"ROMAN_FEDOSEEV\",\"changeDate\":1530822026000,\"changeUser\":\"ROMAN_FEDOSEEV\",\"commonLimit\":{{{{\"distributeChargesRuleCode\":847,\"name\":\"Корпоративный счет, в рамках лимита\",\"applyToCharges\":[{{{{\"type\":\"OTHER_CHARGES\",\"name\":\"Прочие виды начислений\"}}}}],\"limit\":{{{{\"measureUnit\":{{{{\"limitMeasureUnitId\":3,\"name\":\"Внутренняя валюта\",\"abbreviation\":\"руб.\",\"measureType\":{{{{\"measureTypeId\":2,\"name\":\"Деньги\"}}}}}}}},\"break\":20,\"warning\":15}}}},\"childRules\":[]}}}},\"corporateRule\":{{{{\"distributeChargesRuleCode\":848,\"name\":\"Корпоративный счет\",\"applyToCharges\":[{{{{\"type\":\"CALL_CHARGES\",\"profile\":{{{{\"ratingProfileId\":305,\"name\":\"Абонентская плата (профиль АП по тарифу)\",\"measureType\":{{{{\"measureTypeId\":4,\"name\":\"Календарные единицы\"}}}}}}}}}}}},{{{{\"type\":\"PRODUCT_CHARGES\",\"profile\":{{{{\"productProfileId\":401,\"name\":\"Профиль \\\"АП по тарифу\\\"\"}}}}}}}}],\"childRules\":[]}}}},\"personalRule\":{{{{\"distributeChargesRuleCode\":849,\"name\":\"Персональный счет\",\"applyToCharges\":[],\"childRules\":[]}}}}}}}}}}}}]&cpohId=0&processingMode=1&subsId={simcards}".format(date=date,simcards=simcards, limit=limit)
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
        dateFrom = datetime.datetime.utcnow().strftime('%d.%m.%Y')
        dateTo = (datetime.datetime.utcnow()+datetime.timedelta(days=365)).strftime('%d.%m.%Y')
        requestPayload = '&p_basket=-1&notifySubsBySms=&operationType=1&dateFrom={dateFrom} 00:00:00&dateTo={dateTo} 00:00:00&cpohId=0&processingMode=1&subsId={simcards}'.format(dateFrom=dateFrom, dateTo=dateTo,simcards=simcards)
        response = self.__performQuery(requestUrl, requestPayload, contentType="application/x-www-form-urlencoded;charset=UTF-8")

        if response:
            self.log(logging.INFO, "The order for disabling simcards has been successfully created")
            __result = True
        return __result

    def enableSim(self, simlist: list):
        __result = False

        requestUrl = "https://{address}/changelock/order"
        simcards = ','.join([_["id"] for _ in simlist])
        date = datetime.datetime.utcnow().strftime('%d.%m.%Y')
        requestPayload = '&p_basket=-1&notifySubsBySms=&operationType=0&dateFrom={date} 00:00:00&cpohId=0&processingMode=1&subsId={simcards}'.format(date=date,simcards=simcards)
        response = self.__performQuery(requestUrl, requestPayload, contentType="application/x-www-form-urlencoded;charset=UTF-8")

        if response:
            self.log(logging.INFO, "The order for disabling simcards has been successfully created")
            __result = True

        return __result

    def setSimPlan(self, simlist: list, plan: string):
        """
            Replaces active plan for listed simcards with the given by plan id
            e.g. 3382 - "Интернет Вещей"
                 1770 - "Интернет вещей 2018"
        """

        __result = False

        requestUrl = "https://{address}/changepkgrateplan/order"
        simcards = ','.join([_["id"] for _ in simlist])
        date = datetime.datetime.utcnow().strftime('%d.%m.%Y')
        requestPayload = '&p_basket=-1&packList=&cpohId=0&processingMode=1&p_excludeSubs=&rtplId={plan}&dateFrom={date}&subsId={simcards}'.format(plan=plan,date=date,simcards=simcards)
        response = self.__performQuery(requestUrl, requestPayload, contentType="application/x-www-form-urlencoded;charset=UTF-8")

        if response and "changeResult" in response and response["changeResult"]["cporId"] > 0:
            self.log(logging.INFO, "The order for simcards' plan change has been successfully created")
            __result = True

        return __result

class MegafonAPIVATS:
    name: str

    __metadata: dict
    __session: Session

    state: State
    __address: string
    __user: string
    __password: string
    simcards: list
    users: list
    json: dict

    __qStats: list

    def __init__(self, address, user, password, name = ""):
        self.name = name
        self.state = State()
        self.__address = address
        self.__user = user
        self.__password = password
        self.__metadata = []
        self.__session = Session()
        self.__session.mount('https://{address}'.format(address=address), MegafonHttpAdapter())
        self.simcards = []
        self.users = []
        self.json = {}
        self.__qStats = {
            "firstQuery": 999999999999,
            "lastQuery": 0,
            "tMinBetweenQueries": 999999999999,
            "count": {}
        }

    def log(self, level, message):
        logging.log(level, "[VATS-{name}] {message}".format(name=self.name, message=message))

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
        self.log(logging.INFO, "Performed {0} queries in {1} seconds with tMinDiff = {2}s".format(qcs, self.__qStats["lastQuery"] - self.__qStats["firstQuery"], self.__qStats["tMinBetweenQueries"]))

    def qWait(self):
        t = time.time() - self.__qStats["lastQuery"]
        while t < (1/QPS):
            time.sleep(0.001)
            t = time.time() - self.__qStats["lastQuery"]

    def __performQuery(self, url: string, payload: string, loginQuery = False, method = "POST", contentType = "application/json", parseRosponseJson = True, timeout = requestTimeout):
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

            fUrl = url.format(address=self.__address, r=r, s = "" if loginQuery else self.__metadata["s"])
            fData = payload.format(address=self.__address, user=self.__user, password=self.__password, authToken = "" if loginQuery else self.__metadata["s"])
            headers = {'Content-Type': contentType}
            if "XSRF_TOKEN" in self.__session.cookies:
                headers["x-csrf-token"] = self.__session.cookies["XSRF_TOKEN"]

            self.log(logging.DEBUG, "[{r}] Performing a request".format(r=r))
            self.log(logging.DEBUG, " [{r}] METHOD: {method}\n [{r}] URL: {url}\n [{r}] CONTENT-TYPE: '{contenttype}'\n [{r}] DATA: {payload}".format(method=method, url=fUrl, payload=fData, contenttype=contentType, r=r))
            try:
                self.__qStatsAdd(fUrl)
                response = self.__session.request(method=method, url=fUrl, data=fData.encode("utf-8"), headers=headers, timeout=timeout)
                response.encoding = "UTF-8"
                self.log(logging.DEBUG, "[{r}] Got {code} status code from server".format(code=response.status_code, r=r))
            except Exception as e:
                loadFailed = True
                self.log(logging.DEBUG, "[{r}] Exception occured during server query. {e}".format(r=r, e=e))
            
            if not loadFailed:
                try:
                    responsePayload = json.loads(response.text) if parseRosponseJson else response.text
                except Exception as e:
                    self.log(logging.ERROR, "[{r}] Failed load response JSON: {e}".format(e=e, r=r))
                    self.log(logging.DEBUG, "[{r}]  DATA: {d}".format(d=response.text, r=r))
                    loadFailed = True
                finally:
                    response.close()

        if not loginQuery and (loadFailed or not self.state.loggedin or response.status_code != 200 or loadFailed or (parseRosponseJson and "error" in responsePayload and responsePayload["error"])):
            self.log(logging.WARNING, "[{r}] Failed getting response from server".format(r=r))
            self.log(logging.DEBUG, " [{r}] LOGIN QUERY: {loginquery}\n [{r}] LOGGED IN: {loggedin}\n [{r}] STATUS CODE: {statuscode}\n [{r}] LOAD FAILED: {loadfailed}\n [{r}] PAYLOAD: {payload}". format(
                r=r,
                loginquery=loginQuery,
                loggedin=self.state.loggedin,
                statuscode=response.status_code if response else "",
                loadfailed=loadFailed,
                payload=responsePayload
            ))
            if not self.state.loggedin or (response and ((response.status_code == 200 and not loadFailed) or (response.status_code == 401) or (response.status_code == 403))):
                if (not self.state.loggedin or response.status_code == 401 or response.status_code == 403 or (parseRosponseJson and (responsePayload["error"] == "NOT_AUTHENTICATED"))) and not loginQuery:
                    self.log(logging.INFO, "[{r}] Not authenticated. Trying to login".format(r=r))
                    if self.__login():
                        responsePayload = self.__performQuery(url, payload, loginQuery=loginQuery, method=method, parseRosponseJson=parseRosponseJson, contentType=contentType, timeout=timeout)
                        success = True

        else:
            success = True

        return responsePayload if success else None

    def __login(self) -> bool:
        try:
            self.state.loggedin = False
            requestUrl = "https://{address}/Sys/itlsysrpc.wcgp?__r={r}"
            requestPayload = '{{"s":null,"u":null,"d":[{{"n":"webauth::auth","p":[{{"d":"{address}","u":"{user}","p":"{password}","a":"Megafon3"}}]}}]}}'

            self.log(logging.INFO, "Loggin into the Megafon VATS")
            try:
                response = self.__performQuery(requestUrl, requestPayload, loginQuery=True)
                if response:
                    self.__metadata = response[0]
                    self.state.loggedin =  True
                    self.log(logging.INFO, "Successfully authorized")
                else:
                    raise Exception("Got empty response from server")
            except Exception as e:
                self.log(logging.INFO, "Authorization failed. {0}".format(e))
        except Exception as e:
            self.state.loggedin = False
            self.log(logging.WARNING, "Unable to login. [{exception}]".format(exception=e))
        return self.state.loggedin

    def getSimCards(self) -> bool:
        result = False

        requestUrl = "https://{address}/Sys/itlsysrpc.wcgp?__r={r}"
        requestPayload = '{{"s":"{authToken}","u":"{user}","d":[{{"n":"itlcs_telnums_sys::list","p":[]}}]}}'

        self.log(logging.INFO, "Attempting to retrieve the list of connected SIM cards")
        response = self.__performQuery(requestUrl, requestPayload)

        if response:
            self.simcards = response[0]
            result = True
        else:
            self.simcards = []
        self.log(logging.INFO, "Got {0} SIM cards from server".format(len(self.simcards)))

        return result

    def getUsers(self) -> bool:
        result = False

        requestUrl = "https://{address}/Session/{s}/itlrpc.wcgp?__r={r}"
        requestPayload = '[{{"n":"itl_accounts::list","p":["c92a42ed-d713-4aa4-9e42-5cecf8803867",1,"name","asc",null,null,10000,false]}}]'

        self.log(logging.INFO, "Attempting to retrieve the list of Users")
        response = self.__performQuery(requestUrl, requestPayload, contentType='text/plain')

        if response and response[0] and response[0]["accounts"]:
            self.users = response[0]["accounts"]
            result = True
        else:
            self.users = []
        self.log(logging.INFO, "Got {0} Users from server".format(len(self.users)))

        return result

    def disableSim(self, simcards) -> bool:
        __result = False

        requestUrl = "https://{address}/Sys/itlsysrpc.wcgp?__r={r}"

        for sim in simcards:
            users = [_ for _ in filter(lambda x: "tn" in x and (x["tn"][0] == sim["tn"]), self.users)]
            if len(users) > 0:
                user = users[0]
                user.pop("tn", None)

        CIDs = [_ for _ in filter(lambda x: x[1] != None and x[1] != '', [ (x[""], x["tn"][0] if "tn" in x else None) for x in self.users])]
        CIDs.insert(0,("", CIDs[0][1]))

        d = [
            {
                "n": "itlcs_telnums_sys::disable",
                "p": list(x["tn"] for x in simcards)
            },
            {
                "n": "itlcs_megafon_sys::removeCompanyTelnum",
                "p": list(x["tn"] for x in simcards)
            },
            {
                "n": "megafon_desktop_state::deactivateTelnum",
                "p": list(x["tn"] for x in simcards)
            },
            {
                "n": "itl_caller_ids::set",
                "p": [dict(CIDs)]
            },
        ]
        requestPayload = '{{{{"s":"{{authToken}}","u":"{{user}}","d":{d}}}}}'.format(d=json.dumps(d, ensure_ascii=False).replace('{', '{{').replace('}','}}'))

        self.log(logging.INFO, "Attempting to disable simcards")
        response = self.__performQuery(requestUrl, requestPayload)

        if response:
            __result = True
            self.log(logging.INFO, "Simcards successfully disabled")

        return __result

    def enableSim(self, simcards: list) -> bool:
        __result = False

        requestUrl = "https://{address}/Sys/itlsysrpc.wcgp?__r={r}"

        users = [_ for _ in filter(lambda x: not "tn" in x, self.users)]
        for n in range(len(simcards)):
            simcards[n]["user"] = users[n]
            users[n]["tn"] = [simcards[n]["tn"]]

        CIDs = [_ for _ in filter(lambda x: x[1] != None and x[1] != '', [ (x[""], x["tn"][0] if "tn" in x else None) for x in self.users])]
        CIDs.insert(0,("", CIDs[0][1]))

        d = [
            {
                "n": "megafon_desktop_state::activateTelnum",
                "p": list(x["tn"] for x in simcards)
            },
            {
                "n": "itlcs_megafon_sys::removeCompanyTelnum",
                "p": list(x["tn"] for x in simcards)
            },
            {
                "n": "itlcs_telnums_sys::set",
                "p": list({
                    "disabled": False,
                    "target": x["user"][""],
                    "targetName": x["user"]["n"],
                    "tn": x["tn"],
                    "type": "account",
                    "greeting": None
                } for x in simcards)
            },
            {
                "n": "itl_caller_ids::set",
                "p": [dict(CIDs)]
            },
        ]
        requestPayload = '{{{{"s":"{{authToken}}","u":"{{user}}","d":{d}}}}}'.format(d=json.dumps(d, ensure_ascii=False).replace('{', '{{').replace('}','}}'))

        self.log(logging.INFO, "Attempting to enable simcards")
        response = self.__performQuery(requestUrl, requestPayload)

        if response:
            __result = True
            self.log(logging.INFO, "Simcards successfully enabled")

        return __result

    def connectSim(self, simcards: list) -> bool:
        __result = False

        requestUrl = "https://{address}/Sys/itlsysrpc.wcgp?__r={r}"

        d = [
            {
                "n": "megafon_desktop_state::addNewSimList",
                "p": [
                    [
                    {
                        "target": {
                            "tn": "7" + sim["msisdn"],
                            "company": False,
                            "disabled": True
                        },
                        "sendSMS": None
                    } for sim in simcards
                    ]
                ]
            }
        ]
        requestPayload = '{{{{"s":"{{authToken}}","u":"{{user}}","d":{d}}}}}'.format(d=json.dumps(d, ensure_ascii=False).replace('{', '{{').replace('}','}}'))

        self.log(logging.INFO, "Attempting to connect simcards")
        response = self.__performQuery(requestUrl, requestPayload)

        if response:
            __result = True
            self.log(logging.INFO, "Simcards successfully connected")

        return __result

    def removeSim(self, simcards: list) -> bool:
        __result = True

        requestUrl = "https://{address}/Sys/itlsysrpc.wcgp?__r={r}"

        for sim in simcards:
            d = [
                {
                    "n": "itl_wizard::removeTelnum",
                    "p": [ sim["tn"] ]
                } 
            ]
            requestPayload = '{{{{"s":"{{authToken}}","u":"{{user}}","d":{d}}}}}'.format(d=json.dumps(d, ensure_ascii=False).replace('{', '{{').replace('}','}}'))

            self.log(logging.INFO, "Attempting to remove simcard {sim}".format(sim=sim["tn"]))
            response = self.__performQuery(requestUrl, requestPayload)

            if response:
                self.log(logging.INFO, "Simcard {sim} successfully removed".format(sim=sim["tn"]))
            else:
                __result = False
                self.log(logging.INFO, "Simcard {sim} removeing failed".format(sim=sim["tn"]))

        return __result

    def deleteUser(self, users: list) -> bool:
        __result = True

        requestUrl = "https://{address}/Sys/itlsysrpc.wcgp?__r={r}"


        for user in users:
            d = [
                {
                    "n": "itl_accounts_sys::remove",
                    "p": [ user[""] ]
                } 
            ]
            requestPayload = '{{{{"s":"{{authToken}}","u":"{{user}}","d":{d}}}}}'.format(d=json.dumps(d, ensure_ascii=False).replace('{', '{{').replace('}','}}'))

            self.log(logging.INFO, "Attempting to delete user {user}/{userName}".format(user=user[""], userName=user["n"]))
            response = self.__performQuery(requestUrl, requestPayload)

            if response:
                self.log(logging.INFO, "User {user} successfully removed".format(user=user[""]))
            else:
                __result = False
                self.log(logging.INFO, "User {user} removeing failed".format(user=user[""]))

        return __result        