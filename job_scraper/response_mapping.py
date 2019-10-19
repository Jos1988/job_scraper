from requests import Response


class ResponseMapper:

    @staticmethod
    def map(monster_id: int, response: dict):

        data = {}
        try:
            data['sourceId'] = monster_id
            data['source'] = 'monster.com'
            data['responseData'] = str(response)
            data['title'] = str(response['companyInfo']['companyHeader'])
            data['description'] = str(response['jobDescription'])

            data['location'] = '-'
            if 'jobLocation' in response['companyInfo']:
                data['location'] = str(response['companyInfo']['jobLocation'])

            data['jobLocationCountry'] = '-'
            if 'jobLocationCountry' in response:
                data['jobLocationCountry'] = str(response['jobLocationCountry'])

            data['jobLocationRegion'] = '-'
            if 'jobLocationRegion' in response:
                data['jobLocationRegion'] = str(response['jobLocationRegion'])

            data['jobLocationCity'] = '-'
            if 'jobLocationCity' in response:
                data['jobLocationCity'] = str(response['jobLocationCity'])

            data['name'] = '-'
            if 'name' in response['companyInfo']:
                data['name'] = str(response['companyInfo']['name'])

            data['category'] = '-'
            if 'jobCategory' in response:
                data['category'] = str(response['jobCategory'])

            data['ccCategory'] = '-'
            if 'jobOccCategory' in response:
                data['ccCategory'] = str(str(response['jobOccCategory']))

            data['cao'] = '-'
            if 'isCao' in response:
                data['cao'] = str(str(response['isCao']))

            data['jobIndustry'] = '-'
            if 'jobIndustry' in response:
                data['jobIndustry'] = str(response['jobIndustry'])

            data['proDiversity'] = '-'
            if 'summary' in data and 'proDiversity' in response['summary']:
                data['proDiversity'] = str(response['summary']['proDiversity'])

        except:
            raise RuntimeError(f'error parsing data from: "{monster_id}"')

        return data
