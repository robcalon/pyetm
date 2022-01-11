import json

class Response:
    
    async def _check_response(self, response):
        """Handle passed error responses from the ETM server."""

        if not (response.status <= 400):

            try: 
                # get error message
                error = await response.json()
                error = error.get('errors')

                # help to understand ETM error message
                print(f'ClientResponseError: {response.status} ' + 
                      f'- {response.reason}')

                nr = len(str(response.status)) + len(response.reason) + 24
                print(nr * '-')

                print('ErrorMessage(s):')
                for message in error:
                    print(f'--> {message}')
                print()

            except:
                # get error as text
                message = response.text()
                print(message)

            finally:    
                # raise status error
                response.raise_for_status()