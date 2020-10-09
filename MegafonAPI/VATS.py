import logging
import json
import random
import time
import datetime
from requests import Session
from MegafonAPI import State, BaseConsts, HttpAdapter as MegafonHttpAdapter

class VATS:
    name: str

    __metadata: dict
    __session: Session

    state: State
    __address: str
    __user: str
    __password: str
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