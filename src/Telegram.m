#import "Telegram.h"

@implementation Telegram

- (instancetype)initWithToken:(NSString *)token andAppId:(NSString *)appId andAppHash:(NSString *)appHash
{
    self = [super init];

    if (self)
        {
            td_set_log_verbosity_level(0);
            client    = td_json_client_create();
            is_closed = 0;
            _token    = token;
            _appId    = appId;
            _appHash  = appHash;
        }

    return self;
}

- (void)main
{
    ENTER_POOL
    const double WAIT_TIMEOUT = 1.0;

    while (!is_closed)
        {
            const char *result = td_json_client_receive(client, WAIT_TIMEOUT);

            NS_DURING
            {
                if (result)
                    {
                        NSData *jsonData =
                            [[NSString stringWithFormat:@"%s", result] dataUsingEncoding:NSUTF8StringEncoding];

                        NSDictionary *jsonDict = [NSJSONSerialization JSONObjectWithData:jsonData
                                                                                 options:NULL
                                                                                   error:nil];

                        NSString *type = [jsonDict objectForKey:@"@type"];

                        //
                        // Message handling
                        //
                        [self updateNewMessage:jsonDict type:type];

                        // updateAuthorizationState
                        if ([type isEqualToString:@"updateAuthorizationState"])
                            {
                                NSDictionary *authorizationState = [jsonDict objectForKey:@"authorization_state"];
                                NSString *authorizationStateType = [authorizationState objectForKey:@"@type"];

                                // authorizationStateWaitTdlibParameters
                                [self useParameters:jsonDict type:authorizationStateType];

                                // authorizationStateWaitEncryptionKey
                                [self useEncryptionKey:jsonDict type:authorizationStateType];

                                // authorizationStateWaitPhoneNumber
                                [self usePhoneNumber:jsonDict type:authorizationStateType];

                                // authorizationStateReady
                                if ([authorizationStateType isEqualToString:@"authorizationStateReady"])
                                    {
                                        NSLog(@"Connected");
                                    }
                            }
                    }
            }
            NS_HANDLER
            {
                fprintf(stderr, "%s\n", [[localException description] UTF8String]);
                // exit(EXIT_FAILURE);
            }
            NS_ENDHANDLER
        }

    td_json_client_destroy(client);

    LEAVE_POOL
}


- (void)send:(NSDictionary *)dict
{
    NSError *error;
    NSData *jsonData = [NSJSONSerialization dataWithJSONObject:dict options:0 error:&error];

    if (error == nil)
        {
            NSString *response = [[NSString alloc] initWithData:jsonData encoding:NSUTF8StringEncoding];

            td_json_client_send(client, [response UTF8String]);
            [response release];
        }

    return;
}

- (void)useParameters:(NSDictionary *)jsonDict type:(NSString *)t
{
    if ([t isEqualToString:@"authorizationStateWaitTdlibParameters"])
        {
            NSDictionary *entry = [[NSDictionary alloc]
                initWithObjectsAndKeys:@"setTdlibParameters", @"@type",
                                       [[[NSDictionary alloc]
                                           initWithObjectsAndKeys:@"Desktop", @"device_model", self->_appId, @"api_id",
                                                                  self->_appHash, @"api_hash",
                                                                  @"en", @"system_language_code", @"1.0",
                                                                  @"system_version", @"1.0", @"application_version",
                                                                  [NSString stringWithFormat:@"%@/.config/"
                                                                                             @"chumba",
                                                                                             NSHomeDirectory()],
                                                                  @"database_directory", nil] autorelease],
                                       @"parameters", nil];

            [self send:entry];
            [entry release];
        }

    return;
}

- (void)useEncryptionKey:(NSDictionary *)jsonDict type:(NSString *)authorizationStateType
{
    if ([authorizationStateType isEqualToString:@"authorizationStateWai"
                                                @"tEncryptionKey"])
        {
            NSDictionary *entry =
                [[NSDictionary alloc] initWithObjectsAndKeys:@"checkDatabaseEncryptionKey", @"@type",
                                                             @"randomencryption", @"encryption_key", nil];

            [self send:entry];
            [entry release];
        }

    return;
}

- (void)usePhoneNumber:(NSDictionary *)jsonDict type:(NSString *)authorizationStateType
{
    if ([authorizationStateType isEqualToString:@"authorizationStateWaitPhoneNumber"])
        {
            NSDictionary *entry = [[NSDictionary alloc]
                initWithObjectsAndKeys:@"checkAuthenticationBotToken", @"@type",
                                       self->_token, @"token", nil];

            [self send:entry];
            [entry release];
        }

    return;
}

- (void)updateNewMessage:(NSDictionary *)jsonDict type:(NSString *)type
{
    if ([type isEqualToString:@"updateNewMessage"])
        {
            NSDictionary *messageDict = [jsonDict objectForKey:@"message"];
            NSString *messageType     = [messageDict objectForKey:@"@type"];
            NSString *chatId          = [messageDict objectForKey:@"chat_id"];
            NSDictionary *contentDict = [messageDict objectForKey:@"content"];
            NSDictionary *contentText = [contentDict objectForKey:@"text"];
            NSString *message         = [contentText objectForKey:@"text"];

            if ([message isEqualToString:@"/start"])
                {
                    //[self sendMessage:@"kek" to:chatId];
                }

            NSLog(@"%@: %@", chatId, message);
        }

    return;
}

//
// Sending message
//
- (void)sendMessage:(NSString *)text to:(NSString *)chatId
{
    NSDictionary *formattedTextDict =
        [[NSDictionary alloc] initWithObjectsAndKeys:@"formattedText", @"@type", text, @"text", nil];

    NSDictionary *inputMessageContent =
        [[NSDictionary alloc] initWithObjectsAndKeys:@"inputMessageText", @"@type", formattedTextDict, @"text", nil];

    NSDictionary *entry =
        [[NSDictionary alloc] initWithObjectsAndKeys:@"sendMessage", @"@type", chatId, @"chat_id", inputMessageContent,
                                                     @"input_message_content", nil];

    [self send:entry];
    [entry release];
    [inputMessageContent release];
    [text release];

    return;
}
@end
