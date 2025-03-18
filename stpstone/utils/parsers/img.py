### METHODS TO HANDLE STRINGS ###

from base64 import b64encode


class ImgHandler:

    def img_to_html(self, complete_path, format='jpeg'):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        # convert imagem to base64
        data_uri = b64encode(open(complete_path, 'rb').read()).decode('utf-8')
        # create tag
        img_tag = '<img src="data:image/{};base64,{}">'.format(format, data_uri)
        # returning image tag
        return img_tag
