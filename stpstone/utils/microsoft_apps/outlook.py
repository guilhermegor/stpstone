### SENDING EMAIL THROUGH OUTLOOK CLIENT ###

import os
import win32com.client as win32
from stpstone.utils.parsers.str import StrHandler
from stpstone.utils.parsers.folders import DirFilesManagement
from stpstone.utils.parsers.json import JsonFiles


class DealingOutlook:

    def send_email(self, mail_subject, mail_to=None, mail_cc=None, mail_bcc=None, mail_body=None,
                   mail_attachments=None, send_behalf_of=None, auto_send_email=False,
                   bl_frame_sender=False, bl_read_receipt=False, bl_delivery_receipt=False,
                   bl_image_display=False, str_html_signature=None):
        """
        REFERENCES: https://stackoverflow.com/questions/63435690/send-outlook-email-with-attachment-to-list-of-users-in-excel-with-python
        DOCSTRING: SEND EMAIL WITH ATTACHMENTS AND ON BEHALF OF AN EMAIL, INSTEAD OF THE DEFAULT
        INPUTS: TO, CC, BCC (2 OUT OF THREE ARE OPTIONAL), SUBJECT, BODY, ATTACHMENTS, SEND ON
            BEHALF OF AND AUTO SEND (Y/N)
        OUTPUTS: -
        """
        # outlook object through win32com
        outlook = win32.Dispatch('outlook.application')
        # create mail item
        mail = outlook.CreateItem(0)
        # to
        if mail_to:
            mail.To = mail_to
        # cc
        if mail_cc:
            mail.CC = mail_cc
        # blind copy
        if mail_bcc:
            mail.BCC = mail_bcc
        # subject
        mail.Subject = mail_subject
        # creating send on behalf-of
        if send_behalf_of:
            #   if the frame email is false the send on behalf of ought change the sender account,
            #       nonetheless it will use the main account with a frame to send the email
            if bl_frame_sender == False:
                for account in outlook.Session.Accounts:
                    if str(account) == send_behalf_of:
                        send_behalf_of_account = account
                        break
                mail._oleobj_.Invoke(*(64209, 0, 8, 0, send_behalf_of_account))
            mail.SentOnBehalfOfName = send_behalf_of
        # mail body if not none
        if mail_body != None:
            #   defining mail body
            mail.HTMLBody = mail_body
            #   loading html signature and appending to html body
            if str_html_signature != None:
                mail.HTMLBody += str_html_signature
        # mail attachments
        if mail_attachments != None:
            for mail_attachment in mail_attachments:
                if os.path.exists(mail_attachment):
                    mail.Attachments.Add(Source=mail_attachment)
        # request read receipt
        mail.ReadReceiptRequested = bl_read_receipt
        # request delivery receipt
        mail.OriginatorDeliveryReportRequested = bl_delivery_receipt
        # display created email
        if auto_send_email == False:
            mail.Display()
        elif auto_send_email == 'Y':
            if bl_image_display:
                try:
                    mail.Display()
                    mail.Send()
                except:
                    pass
            else:
                try:
                    mail.Send()
                except:
                    pass
        else:
            mail.Close

    def download_attch(self, email_account, outlook_folder,
                       subj_sub_string, attch_save_path, bl_save_file_w_original_name=False,
                       list_fileformat=None, outlook_subfolder=None, move_to_folder=None,
                       save_only_first_event=False, bl_leave_after_first_occurance=False,
                       bl_break_loops=False):
        """
        DOCTRING: DOWNLOAD A FILE FROM AN SPECIFIC EMAIL
        INPUTS: EMAIL ACCOUNT, OUTLOOK FOLDER, PART OF SUBJECT SUB STRING, ATTACHMENT SAVING PATH
            MOVE TO AN ESPECIFIC FOLDER, SAVE ONLY FIRST EVENT (FALSE AS DEFAULT)
        OUTPUTS: JSON WITH SAVING STATUS (OK, NOK)
        """
        # defining variables
        out_app = win32.Dispatch('Outlook.Application')
        out_namespace = out_app.GetNamespace('MAPI')
        if outlook_subfolder:
            out_iter_folder = out_namespace.Folders[email_account].Folders[
                outlook_folder].Folders[outlook_subfolder]
        else:
            out_iter_folder = out_namespace.Folders[email_account].Folders[outlook_folder]
        if move_to_folder != None:
            out_move_to_folder = out_namespace.Folders[email_account].Folders[move_to_folder]
        dict_attch_saving_status = dict()
        # defining attachments generator
        if str(type(attch_save_path)) == "<class 'list'>":
            list_attch_save_path = [attch for attch in attch_save_path]
        elif str(type(attch_save_path)) == "<class 'str'>":
            list_attach_save_path = list()
            list_attach_save_path.append(attch_save_path)
            list_attch_save_path = [attch for attch in list_attach_save_path]
        else:
            raise Exception(
                'Attachments saving paths ought be a string or list of strings, please check '
                + 'wheter variable type is valid.')
        # counting all itemns in the sub-folder
        item_count = out_iter_folder.Items.Count
        if item_count > 0:
            for i in range(item_count):
                # in case user wants to download only the first occurance, leave
                if bl_break_loops == True:
                    break
                # defining com object of the current message
                message = out_iter_folder.Items[i]
                # find desired mail item and downloanding its attchments
                if StrHandler().find_substr_str(message.Subject, subj_sub_string):
                    for attch_save_path in list_attch_save_path:
                        # in case user wants to download only the first occurance, leave
                        if bl_break_loops == True:
                            break
                        for attch in message.Attachments:
                            # if save file with original name is true, and the file format is the
                            #   desired one, save it in the local source
                            if bl_save_file_w_original_name == True and \
                                DirFilesManagement().get_file_format_from_file_name(
                                    attch.FileName) in list_fileformat:
                                attch.SaveAsFile(
                                    attch_save_path + attch.FileName)
                                dict_attch_saving_status[attch_save_path + attch.FileName] = \
                                    DirFilesManagement().object_exists(
                                        attch_save_path + attch.FileName)
                            # elif complete path to save file is provided, procceed with saving to
                            #   to local source
                            elif bl_save_file_w_original_name == False:
                                attch.SaveAsFile(attch_save_path)
                                dict_attch_saving_status[attch_save_path] = \
                                    DirFilesManagement().object_exists(attch_save_path)
                            # check wheter is important to move email to another folder
                            if move_to_folder != None:
                                message.Move(out_move_to_folder)
                            # check whether is important to save just the first attachment or not
                            if save_only_first_event == True and \
                                    bool(dict_attch_saving_status) == True:
                                return JsonFiles().send_json(dict_attch_saving_status)
                        # in case user wants to download only the first occurance, leave
                        if bl_leave_after_first_occurance == True:
                            bl_break_loops = True
        # return infos with respect to downloaded attachments
        return JsonFiles().send_json(dict_attch_saving_status)

    def received_email_subject_w_rule(self, email_account, outlook_folder,
                                      subj_sub_string, outlook_subfolder=None,
                                      output='subject'):
        """
        REFERENCES: https://stackoverflow.com/questions/22813814/clearly-documented-reading-of-emails-functionality-with-python-win32com-outlook,
            https://docs.microsoft.com/en-us/dotnet/api/microsoft.office.interop.outlook.mailitem?redirectedfrom=MSDN&view=outlook-pia#properties_
        DOCSTRING: LAST EMAIL SUBJECT WITH RULE (OR NAME LIKE)
        INPUTS: EMAIL ACCOUNT, OUTLOOK FOLDER, SUBJECT SUBSTRING, OUTLOOK FOLDER (NONE AS DEFAULT)
        OUTPUTS: OBJECT
        """
        # defining variables
        out_app = win32.Dispatch('Outlook.Application')
        out_namespace = out_app.GetNamespace('MAPI')
        if outlook_subfolder:
            out_iter_folder = out_namespace.Folders[email_account].Folders[
                outlook_folder].Folders[outlook_subfolder]
        else:
            out_iter_folder = out_namespace.Folders[email_account].Folders[outlook_folder]
        # counting all itemns in the sub-folder
        item_count = out_iter_folder.Items.Count
        # list emails subjects
        list_emails_subjects = list()
        if item_count > 0:
            for i in range(item_count):
                message = out_iter_folder.Items[i]
                # find desireed mail item and downloanding its attchments
                if StrHandler().find_substr_str(message.Subject, subj_sub_string):
                    if output == 'subject':
                        list_emails_subjects.append(message.Subject)
                    elif output == 'message_raw':
                        list_emails_subjects.append(message)
                    elif output == 'properties':
                        list_emails_subjects.append({
                            'subject': message.Subject,
                            'last_edition': message.LastModificationTime,
                            'creation_time': message.CreationTime,
                        })
                    else:
                        raise NameError(f'Output {output} is invalid')
        return list_emails_subjects

    def get_body_content(self, email_account, outlook_folder,
                         subj_sub_string, outlook_subfolder=None):
        """
        REFERENCES: https://stackoverflow.com/questions/22813814/clearly-documented-reading-of-emails-functionality-with-python-win32com-outlook,
            https://docs.microsoft.com/en-us/dotnet/api/microsoft.office.interop.outlook.mailitem?redirectedfrom=MSDN&view=outlook-pia#properties_
        DOCSTRING: GET BODY CONTENT
        INPUTS: EMAIL ACCOUNT, OUTLOOK FOLDER, SUBJECT SUBSTRING, OUTLOOK FOLDER (NONE AS DEFAULT)
        OUTPUTS: OBJECT
        """
        # defining variables
        out_app = win32.Dispatch('Outlook.Application')
        out_namespace = out_app.GetNamespace('MAPI')
        if outlook_subfolder:
            out_iter_folder = out_namespace.Folders[email_account].Folders[
                outlook_folder].Folders[outlook_subfolder]
        else:
            out_iter_folder = out_namespace.Folders[email_account].Folders[outlook_folder]
        # counting all itemns in the sub-folder
        item_count = out_iter_folder.Items.Count
        # list emails subjects
        list_emails_subjects = list()
        if item_count > 0:
            for i in range(item_count):
                message = out_iter_folder.Items[i]
                # find desireed mail item and downloanding its attchments
                if StrHandler().find_substr_str(message.Subject, subj_sub_string):
                    list_emails_subjects.append({
                        'subject': message.Subject,
                        'last_edition': message.LastModificationTime,
                        'creation_time': message.CreationTime,
                        'body': message.body,
                    })
        return list_emails_subjects

    def reply_email(self, email_account, outlook_folder, subj_sub_string, msg_body, mail_cc=None,
                    mail_bcc=None, auto_send_email=False, bl_image_display=False, outlook_subfolder=None):
        """
        DOCSTRING: SEND AN AUTOMATED REPLY
        INPUTS: EMAIL ACCOUNT, OUTLOOK FOLDER, SUBJECT SUBSTRING, MESSAGE BODY, MAIL CC, MAIL BCC,
            AUTO SEND EMAIL, BL IMAGE DISPLAY AND OUTLOOK SUBFOLDER (NONE AS DEFAULT)
        OUTPUTS: OBJECT
        """
        # defining variables
        out_app = win32.Dispatch('Outlook.Application')
        out_namespace = out_app.GetNamespace('MAPI')
        if outlook_subfolder:
            out_iter_folder = out_namespace.Folders[email_account].Folders[
                outlook_folder].Folders[outlook_subfolder]
        else:
            out_iter_folder = out_namespace.Folders[email_account].Folders[outlook_folder]
        # counting all itemns in the sub-folder
        item_count = out_iter_folder.Items.Count
        # iterating through folder
        if item_count > 0:
            for i in range(item_count):
                #   creating message object
                message = out_iter_folder.Items[i]
                #   find desireed mail item and sending a reply
                if message.Subject == subj_sub_string:
                    #   reply object
                    reply = message.Reply()
                    #   creating new body
                    reply.HTMLBody = msg_body + reply.HTMLBody
                    #   breaking loop
                    break
        # cc
        if mail_cc:
            reply.CC = mail_cc
        # blind copy
        if mail_bcc:
            reply.BCC = mail_bcc
        # display created email
        if auto_send_email == False:
            reply.Display()
        elif auto_send_email == 'Y':
            if bl_image_display:
                try:
                    reply.Display()
                    reply.Send()
                except:
                    pass
            else:
                try:
                    reply.Send()
                except:
                    pass
        else:
            reply.Close
