
import pandas as pd
import typeform as tf
from stpstone.handling_data.json_format import JsonFiles


class DealingTypeForm:

    def downloading_response(self, json_file, api_token, form_id, since_date=None, until_date=None):
        '''
        DOCSTRING: DOWNLOAD RESPONSES OF A TYPEFORM TO A NETWORK PATH IN JSON EXTENSION
        INPUTS: JSON FILE (STR), API TOKEN (STR), FORM_ID (STR), SINCE DATE AND UNTIL DATE FORMAT:
        'YYYY-MM-DDT00:00:00.00Z', AS WELL A STR TYPE
        OUTPUTS: 'OK' OR 'NOK' FOR SUCCESSFUL SAVING
        '''

        # fetching data
        form_mem = tf.Typeform(token=api_token).responses.list(uid=form_id,
                                                               since=since_date,
                                                               until=until_date)

        # returning status of saving
        return JsonFiles().dump_message(form_mem, json_file)

    def dictionary_ref_questions(self, api_token, form_id):

        # placing an empty dict in memory
        dict_ref_questions = dict()

        # fetching data of form of interest
        tf_infos = tf.Typeform(token=api_token).forms.get(form_id)

        # building a dict: key - ref / value - question
        dict_ref_questions['response_id'] = '#'

        for field in tf_infos['fields']:
            dict_ref_questions[field['ref']] = field['title']

        dict_ref_questions['landed'] = 'Start Date (UTC)'
        dict_ref_questions['submitted_at'] = 'Submit Date (UTC)'
        dict_ref_questions['network_id'] = 'Network ID'

        return dict_ref_questions

    def response_to_dataframe(self, ref_questions_translator, responses_json_path,
                              api_token, form_id):
        '''
        DOCSTRING: RETURN PANDAS DATAFRAME OF RESPONSE TYPEFORM 
        INPUTS: DICT OF REFS KEYS X QUESTIONS VALUES, 
        RESPONSE PATH (OUGHT BE DOWNLOADED TO NETWORK PATH), API TOKEN (STR) AND FORM ID (STR)
        OUTPUTS: LIST OF PILED RESPONSES
        '''

        # import json to memory
        responses_json_mem = JsonFiles().load_message(responses_json_path)

        # creating memory slots
        list_piled_responses = list()
        dict_current_responses = dict()

        # aggregate dicts in a list
        for item in responses_json_mem['items']:
            for _, value in ref_questions_translator.items():
                dict_current_responses[value] = None
            if item['response_id']:
                dict_current_responses['#'] = item['response_id']
            if item['landed_at']:
                dict_current_responses['Start Date (UTC)'] = item['landed_at']
            if item['submitted_at']:
                dict_current_responses['Submit Date (UTC)'] = item['submitted_at']
            if item['metadata']['network_id']:
                dict_current_responses['Network ID'] = item['metadata']['network_id']
            for answer in item['answers']:
                if answer['type'] == 'choices':
                    dict_current_responses[ref_questions_translator[
                        answer['field']['ref']]] = answer[
                        answer['type']]['labels']
                else:
                    dict_current_responses[ref_questions_translator[
                        answer['field']['ref']]] = answer[answer['type']]
            list_piled_responses.append(dict_current_responses.copy())

        # exporting list to dataframe
        return pd.DataFrame(list_piled_responses)
