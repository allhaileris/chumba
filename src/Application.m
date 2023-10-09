#import "Application.h"
#import "Env.h"

@implementation Application

- (instancetype)init
{
    self = [super init];

    if (self)
        {
            Env *env          = [[Env alloc] initWithFile:@".env"];
            NSString *token   = [env getParameter:@"TOKEN"];
            NSString *appId   = [env getParameter:@"APP_ID"];
            NSString *appHash = [env getParameter:@"APP_HASH"];

            telegram = [[Telegram alloc] initWithToken:token andAppId:appId andAppHash:appHash];
            [telegram start];
        }

    return self;
}

- (void)handleMessage:(NSString *)message
{
    NSLog(@"%@", message);
}

- (void)dealloc
{
    [super dealloc];
}

@end
